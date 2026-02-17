#!/usr/bin/env python3
"""
ETC Document Downloader
=======================

Downloads and archives all available documents from Invesco and WisdomTree
product pages for the Physical Silver ETCs.

Behaviour:
  - Discovers documents by scraping each provider's website.
  - Compares remote content against local files using SHA-256 hashes.
  - If a file is new → saves it directly.
  - If a file has changed → keeps the old version with a date suffix,
    then saves the new version under the canonical name.
  - If a file is unchanged → skips it.
  - Never deletes anything.

Can be run standalone or imported and called from fetch_and_verify_barlists.py.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from datetime import datetime
from html import unescape
from typing import Any

from curl_cffi import requests as cffi_requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOCUMENTS_DIR = os.path.join(SCRIPT_DIR, "documents")

INVESCO_BASE_URL = "https://www.invesco.com"
INVESCO_PRODUCT_PAGE = (
	"https://etf.invesco.com/gb/private/en/product/"
	"invesco-physical-silver-etc/overview"
)
WISDOMTREE_REGULATORY_PAGE = (
	"https://www.wisdomtree.eu/en-gb/resource-library/"
	"prospectus-and-regulatory-reports"
)
WISDOMTREE_FACTSHEET_PAGE = (
	"https://www.wisdomtree.eu/en-gb/resource-library/"
	"fact-sheets-and-reports"
)

ISIN_WISDOMTREE = "JE00B1VS3333"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256(data: bytes) -> str:
	return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str | None:
	if not os.path.isfile(path):
		return None
	h = hashlib.sha256()
	with open(path, "rb") as f:
		for chunk in iter(lambda: f.read(1 << 16), b""):
			h.update(chunk)
	return h.hexdigest()


def _fetch(url: str, timeout: int = 120) -> bytes | None:
	"""Download content with curl_cffi, returns bytes or None on failure."""
	browsers = ["chrome", "chrome110", "chrome120", "safari"]
	for browser in browsers:
		try:
			r = cffi_requests.get(
				url, impersonate=browser, timeout=timeout, allow_redirects=True
			)
			if r.status_code == 200 and len(r.content) > 500:
				return r.content
		except Exception:
			continue
	return None


def _fetch_text(url: str, timeout: int = 30) -> str | None:
	data = _fetch(url, timeout)
	return data.decode("utf-8", errors="replace") if data else None


def _is_pdf(data: bytes) -> bool:
	return data[:4] == b"%PDF"


# ---------------------------------------------------------------------------
# Document discovery — Invesco
# ---------------------------------------------------------------------------


def _discover_invesco_documents() -> list[dict[str, str]]:
	"""
	Scrape the Invesco product page SPA model JSON for all document paths.
	Returns list of {category, filename, url}.
	"""
	html = _fetch_text(INVESCO_PRODUCT_PAGE)
	if not html:
		print("  WARNING: Could not fetch Invesco product page")
		return []

	docs: list[dict[str, str]] = []
	seen_paths: set[str] = set()

	# Extract document paths from the SPA model JSON
	for m in re.finditer(r'data-model-json="([^"]+)"', html):
		decoded = unescape(m.group(1))
		model_text = json.dumps(json.loads(decoded)) if decoded.startswith("{") else decoded

		for dm in re.finditer(r'"documentPath"\s*:\s*"([^"]+)"', model_text):
			dam_path = dm.group(1)
			if dam_path in seen_paths:
				continue
			seen_paths.add(dam_path)

			url = INVESCO_BASE_URL + dam_path
			fname = dam_path.split("/")[-1]
			category = _categorise_invesco_path(dam_path)
			docs.append({"category": category, "filename": fname, "url": url})

	return docs


def _categorise_invesco_path(path: str) -> str:
	"""Map an Invesco DAM path to a local subfolder name."""
	p = path.lower()
	if "bar-list" in p:
		return "bar_lists"
	if "factsheet" in p:
		return "factsheets"
	if "/kid/" in p:
		return "kid"
	if "prospectus" in p:
		return "prospectus"
	if "audit-report" in p:
		return "audit_reports"
	if "ssb-cert" in p:
		return "shariah_certificates"
	if "emt-report" in p or "ept-report" in p:
		return "emt_ept_reports"
	if "constitution" in p:
		return "constitution"
	if "metal-entitlement" in p or "entitlement" in p:
		return "entitlements"
	if "annual-financial-report" in p:
		return "financial_reports/annual"
	if "interim-financial-report" in p:
		return "financial_reports/interim"
	if "reportable-income" in p:
		return "reportable_income"
	return "other"


# ---------------------------------------------------------------------------
# Document discovery — WisdomTree
# ---------------------------------------------------------------------------


def _discover_wisdomtree_documents() -> list[dict[str, str]]:
	"""
	Scrape WisdomTree pages for document links related to Physical Silver.
	Returns list of {category, filename, url}.
	"""
	docs: list[dict[str, str]] = []
	seen_urls: set[str] = set()

	def _add(category: str, url: str, filename: str | None = None):
		if url in seen_urls:
			return
		seen_urls.add(url)
		if not filename:
			filename = url.split("/")[-1].split("?")[0]
			filename = filename.replace("---", "-").replace("--", "-")
		docs.append({"category": category, "filename": filename, "url": url})

	# Bar list (dataspanapi)
	_add(
		"bar_lists",
		f"https://dataspanapi.wisdomtree.com/pdr/documents/METALBAR/MSL/UK/EN-GB/{ISIN_WISDOMTREE}/",
		"wisdomtree_silver_bar_list.pdf",
	)

	# Factsheet
	factsheet_page = _fetch_text(WISDOMTREE_FACTSHEET_PAGE)
	if factsheet_page:
		for m in re.finditer(
			rf'href="(https://dataspanapi\.wisdomtree\.com/pdr/documents/FACTSHEET/MSL/[^"]*{ISIN_WISDOMTREE}[^"]*)"',
			factsheet_page, re.I,
		):
			_add("factsheets", m.group(1), "wisdomtree_physical_silver_factsheet.pdf")

	# Regulatory page — prospectuses + vault inspection letters
	reg_page = _fetch_text(WISDOMTREE_REGULATORY_PAGE)
	if reg_page:
		# Prospectuses
		for m in re.finditer(r'href="([^"]+metal-securities-limited\.pdf[^"]*)"', reg_page, re.I):
			_add("prospectus", m.group(1))
		for m in re.finditer(r'href="([^"]+hedged-metal-securities-limited\.pdf[^"]*)"', reg_page, re.I):
			_add("prospectus", m.group(1))

		# Vault inspection letters — all silver-related patterns
		vault_patterns = [
			r'href="([^"]+vault-inspection-letter[^"]*silver[^"]*\.pdf[^"]*)"',
			r'href="([^"]+wisdomtree-physical-silver[^"]*\.pdf[^"]*)"',
			r'href="([^"]+core-physical-silver[^"]*\.pdf[^"]*)"',
			r'href="([^"]+acc---19235[^"]*\.pdf[^"]*)"',
		]
		for pat in vault_patterns:
			for m in re.finditer(pat, reg_page, re.I):
				_add("vault_inspection_letters", m.group(1))

	return docs


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------


class SyncStats:
	def __init__(self):
		self.new = 0
		self.updated = 0
		self.unchanged = 0
		self.failed = 0
		self.details: list[str] = []

	@property
	def total(self):
		return self.new + self.updated + self.unchanged + self.failed


def _sync_document(
	doc: dict[str, str],
	provider_dir: str,
	stats: SyncStats,
) -> None:
	"""
	Download a single document and sync it into the local folder.
	- New file → save directly.
	- Changed file → archive old version with date, save new as canonical name.
	- Unchanged → skip.
	"""
	category = doc["category"]
	filename = doc["filename"]
	url = doc["url"]

	dest_dir = os.path.join(provider_dir, category)
	os.makedirs(dest_dir, exist_ok=True)
	dest_path = os.path.join(dest_dir, filename)

	# Download
	data = _fetch(url)
	if data is None:
		stats.failed += 1
		stats.details.append(f"  FAIL  download failed  {filename}")
		return

	# For PDFs, verify the content is actually a PDF (not an error page)
	if filename.endswith(".pdf") and not _is_pdf(data):
		stats.failed += 1
		stats.details.append(f"  FAIL  not a valid PDF  {filename}")
		return

	remote_hash = _sha256(data)
	local_hash = _sha256_file(dest_path)

	if local_hash is None:
		# New file
		with open(dest_path, "wb") as f:
			f.write(data)
		stats.new += 1
		stats.details.append(f"  NEW   {len(data):>10,} bytes  {filename}")

	elif local_hash == remote_hash:
		# Unchanged
		stats.unchanged += 1

	else:
		# Changed — archive the old file with a date suffix, save new one
		name, ext = os.path.splitext(filename)
		date_tag = datetime.now().strftime("%Y%m%d")

		# Find a unique archive name (in case we run multiple times per day)
		archive_name = f"{name}_{date_tag}_prev{ext}"
		archive_path = os.path.join(dest_dir, archive_name)
		counter = 1
		while os.path.exists(archive_path):
			archive_name = f"{name}_{date_tag}_prev{counter}{ext}"
			archive_path = os.path.join(dest_dir, archive_name)
			counter += 1

		# Move old → archive
		shutil.move(dest_path, archive_path)

		# Save new
		with open(dest_path, "wb") as f:
			f.write(data)

		stats.updated += 1
		stats.details.append(
			f"  UPD   {len(data):>10,} bytes  {filename}"
			f"  (old → {archive_name})"
		)


def sync_provider(
	provider: str,
	documents: list[dict[str, str]],
	*,
	verbose: bool = True,
) -> SyncStats:
	"""Sync all documents for a single provider."""
	provider_dir = os.path.join(DOCUMENTS_DIR, provider)
	stats = SyncStats()

	for doc in documents:
		_sync_document(doc, provider_dir, stats)
		time.sleep(0.2)  # be polite

	if verbose:
		# Only print details for new/updated/failed items
		for line in stats.details:
			print(line)
		print(
			f"  --- {provider}: {stats.new} new, {stats.updated} updated, "
			f"{stats.unchanged} unchanged, {stats.failed} failed "
			f"(of {stats.total} discovered)"
		)

	return stats


# ---------------------------------------------------------------------------
# Public API (called from fetch_and_verify_barlists.py or standalone)
# ---------------------------------------------------------------------------


def sync_all_documents(*, verbose: bool = True) -> dict[str, SyncStats]:
	"""
	Discover and sync all documents for both providers.
	Returns dict of provider → SyncStats.
	"""
	results: dict[str, SyncStats] = {}

	if verbose:
		print()
		print("=" * 70)
		print("  DOCUMENT SYNC")
		print("=" * 70)

	# --- Invesco ---
	if verbose:
		print("\n  Discovering Invesco documents...")
	inv_docs = _discover_invesco_documents()
	if verbose:
		print(f"  Found {len(inv_docs)} documents")
	results["invesco"] = sync_provider("invesco", inv_docs, verbose=verbose)

	# --- WisdomTree ---
	if verbose:
		print(f"\n  Discovering WisdomTree documents...")
	wt_docs = _discover_wisdomtree_documents()
	if verbose:
		print(f"  Found {len(wt_docs)} documents")
	results["wisdomtree"] = sync_provider("wisdomtree", wt_docs, verbose=verbose)

	if verbose:
		total_new = sum(s.new for s in results.values())
		total_upd = sum(s.updated for s in results.values())
		total_fail = sum(s.failed for s in results.values())
		print(f"\n  Document sync complete: {total_new} new, {total_upd} updated, {total_fail} failed")
		print("=" * 70)

	return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
	import argparse

	parser = argparse.ArgumentParser(
		description="Download and archive ETC fund documents"
	)
	parser.add_argument(
		"--provider",
		choices=["invesco", "wisdomtree"],
		nargs="+",
		default=["invesco", "wisdomtree"],
		help="Which providers to sync (default: both)",
	)
	parser.add_argument(
		"--quiet", "-q",
		action="store_true",
		help="Suppress output except errors",
	)
	args = parser.parse_args()

	verbose = not args.quiet

	if "invesco" in args.provider and "wisdomtree" in args.provider:
		sync_all_documents(verbose=verbose)
	else:
		if verbose:
			print()
			print("=" * 70)
			print("  DOCUMENT SYNC")
			print("=" * 70)

		for provider in args.provider:
			if verbose:
				print(f"\n  Discovering {provider} documents...")

			if provider == "invesco":
				docs = _discover_invesco_documents()
			else:
				docs = _discover_wisdomtree_documents()

			if verbose:
				print(f"  Found {len(docs)} documents")
			sync_provider(provider, docs, verbose=verbose)

		if verbose:
			print("=" * 70)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
