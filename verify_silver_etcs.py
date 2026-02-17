#!/usr/bin/env python3
"""
ETC Silver Bar Inventory Verification
=====================================

Focus areas implemented:
  1) Pull data on individual silver bars from ETC bar lists
  2) Build normalized/sorted JSON of all bars and aggregates
  3) Compare bar-list physical silver with expected silver from fund metrics

Notes:
  - Many ETF/ETC providers geo-block automated downloads.
  - This script supports both download URLs and manually placed local files.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pdfplumber
import pypdf


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")


DEFAULT_FUNDS: dict[str, dict[str, Any]] = {
	"invesco": {
		"display_name": "Invesco Physical Silver ETC",
		"isin": "IE00B43VDT70",
		"ticker": "SSLV.L",
		"bar_list_url": "https://www.invesco.com/content/dam/invesco/emea/en/product-documents/etf/share-class/bar-list/IE00B43VDT70_bar-list_en.pdf",
		"local_pdf": os.path.join(CACHE_DIR, "invesco_silver_barlist.pdf"),
	},
	"wisdomtree": {
		"display_name": "WisdomTree Physical Silver ETC",
		"isin": "JE00B1VS3333",
		"ticker": "PHAG.L",
		"bar_list_url": "https://dataspanapi.wisdomtree.com/pdr/documents/METALBAR/MSL/UK/EN-GB/JE00B1VS3333/",
		"local_pdf": os.path.join(CACHE_DIR, "wisdomtree_silver_barlist.pdf"),
	},
}





@dataclass
class BarRecord:
	serial_number: str
	refiner: str | None
	gross_oz: float | None
	fine_oz: float | None
	fineness: float | None
	vault: str | None
	year: int | None
	source_page: int
	raw_line: str

	def to_dict(self) -> dict[str, Any]:
		return {
			"serial_number": self.serial_number,
			"refiner": self.refiner,
			"gross_oz": self.gross_oz,
			"fine_oz": self.fine_oz,
			"fineness": self.fineness,
			"vault": self.vault,
			"year": self.year,
			"source_page": self.source_page,
			"raw_line": self.raw_line,
		}


def ensure_cache_dir() -> None:
	os.makedirs(CACHE_DIR, exist_ok=True)


def now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def clean_number(raw: str) -> float | None:
	if raw is None:
		return None
	token = raw.replace(",", "").strip()
	if token in {"", "-", "--"}:
		return None
	try:
		return float(token)
	except ValueError:
		return None


def download_pdf(url: str, destination: str, timeout: int = 120) -> str:
	"""Download a PDF using curl_cffi with browser TLS impersonation.

	Bypasses Cloudflare and similar bot-detection by presenting a
	real-browser TLS fingerprint.  Raises RuntimeError on failure.
	"""
	from curl_cffi import requests as cffi_requests

	impersonates = ["chrome", "chrome110", "chrome120", "safari"]
	errors: list[str] = []

	for browser in impersonates:
		try:
			resp = cffi_requests.get(
				url,
				impersonate=browser,
				timeout=timeout,
				allow_redirects=True,
				headers={
					"Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
					"Accept-Language": "en-GB,en;q=0.9",
				},
			)
		except Exception as exc:
			errors.append(f"{browser}: {exc}")
			continue

		if resp.status_code != 200:
			errors.append(f"{browser}: HTTP {resp.status_code}")
			continue

		if len(resp.content) < 1024:
			errors.append(f"{browser}: response too small ({len(resp.content)} bytes)")
			continue

		if b"just a moment" in resp.content[:2000].lower():
			errors.append(f"{browser}: Cloudflare challenge page")
			continue

		if not resp.content[:5].startswith(b"%PDF-"):
			ctype = resp.headers.get("content-type", "")
			if "pdf" not in ctype and "octet-stream" not in ctype:
				errors.append(f"{browser}: unexpected content-type: {ctype}")
				continue

		os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
		with open(destination, "wb") as fh:
			fh.write(resp.content)

		size = len(resp.content)
		print(f"  [curl_cffi] Downloaded {size:,} bytes via {browser}")
		return f"ok_curl_cffi_{browser}"

	raise RuntimeError(
		f"Failed to download PDF from {url}\n"
		+ "\n".join(f"  {e}" for e in errors)
	)


def _sha256(data: bytes) -> str:
	return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str | None:
	if not os.path.exists(path):
		return None
	with open(path, "rb") as f:
		return _sha256(f.read())


def _extract_barlist_date(data: bytes) -> str | None:
	"""Extract the as_of_date from a bar-list PDF's first page (quick parse)."""
	try:
		with pdfplumber.open(io.BytesIO(data)) as pdf:
			if not pdf.pages:
				return None
			text = pdf.pages[0].extract_text() or ""
		fmt = _detect_pdf_format(text)
		if fmt == "wisdomtree":
			meta = _parse_wisdomtree_header(text)
		elif fmt == "invesco":
			meta = _parse_invesco_header(text)
		else:
			return None
		return meta.get("as_of_date")
	except Exception:
		return None


def _normalise_date_tag(raw_date: str | None) -> str:
	"""Turn an as_of_date like '13 February 2026' or '2026-02-13' into YYYYMMDD."""
	if not raw_date:
		return datetime.now().strftime("%Y%m%d")
	# ISO format: 2026-02-13
	m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
	if m:
		return f"{m.group(1)}{m.group(2)}{m.group(3)}"
	# WisdomTree style: 13 February 2026
	try:
		dt = datetime.strptime(raw_date, "%d %B %Y")
		return dt.strftime("%Y%m%d")
	except ValueError:
		pass
	return datetime.now().strftime("%Y%m%d")


def _archive_barlist(local_pdf: str, old_data: bytes) -> str | None:
	"""Archive old bar-list data with a date-stamped filename.

	Returns the archive path if written, or None if already archived.
	"""
	date_str = _extract_barlist_date(old_data)
	tag = _normalise_date_tag(date_str)

	name, ext = os.path.splitext(local_pdf)
	archive_path = f"{name}_{tag}{ext}"

	# Don't create duplicate archives
	if os.path.exists(archive_path):
		existing_hash = _sha256_file(archive_path)
		if existing_hash == _sha256(old_data):
			return archive_path  # identical archive already exists
		# Same date but different content — add counter
		counter = 1
		while os.path.exists(archive_path):
			archive_path = f"{name}_{tag}_{counter}{ext}"
			counter += 1

	with open(archive_path, "wb") as f:
		f.write(old_data)
	print(f"  [archive] Saved previous bar list → {os.path.basename(archive_path)}")
	return archive_path


def find_all_barlists(fund_key: str) -> list[str]:
	"""Return all bar-list PDFs for a fund (dated archives + current), oldest first."""
	cfg = DEFAULT_FUNDS[fund_key]
	canonical = cfg["local_pdf"]
	name, ext = os.path.splitext(canonical)
	pattern = f"{name}_*{ext}"
	paths = sorted(glob.glob(pattern))
	# Append canonical (latest) at the end so it's processed last
	if os.path.exists(canonical) and canonical not in paths:
		paths.append(canonical)
	return paths


def _metrics_file_for_fund(fund_key: str) -> str:
	"""Return the canonical per-fund metrics file path."""
	return os.path.join(CACHE_DIR, f"etc_fund_metrics_{fund_key}.json")


def find_all_metrics_files_for_fund(fund_key: str) -> list[str]:
	"""Return all metrics files for *fund_key* (archives + current), oldest first."""
	canonical = _metrics_file_for_fund(fund_key)
	name, ext = os.path.splitext(canonical)
	# Archives: etc_fund_metrics_invesco_20260217.json, …_20260217_1.json
	pattern = f"{name}_*{ext}"
	paths = sorted(glob.glob(pattern))
	if os.path.exists(canonical) and canonical not in paths:
		paths.append(canonical)
	return paths


def _metrics_date_tag(metrics_path: str) -> str:
	"""Extract data-date tag from a per-fund metrics file.

	Filename convention: ``etc_fund_metrics_<fund>_YYYYMMDD.json``
	Falls back to reading ``as_of`` from file content.
	"""
	base = os.path.basename(metrics_path)
	m = re.search(r"_(\d{8})(?:_\d+)?\.json$", base)
	if m:
		return m.group(1)
	# Canonical file — read as_of from content (flat dict)
	try:
		with open(metrics_path, "r", encoding="utf-8") as f:
			data = json.load(f)
		as_of = data.get("as_of", "")
		if as_of:
			return as_of.replace("-", "")
	except Exception:
		pass
	return "99999999"


def find_metrics_for_fund(fund_key: str, barlist_date_tag: str) -> dict[str, Any]:
	"""Load the per-fund metrics whose data date matches *barlist_date_tag* exactly.

	Returns the flat metrics dict, or empty dict if no same-day match.
	"""
	all_files = find_all_metrics_files_for_fund(fund_key)
	if not all_files:
		print(f"  ERROR: No metrics files found for {fund_key}", file=sys.stderr)
		return {}

	tagged = [(_metrics_date_tag(f), f) for f in all_files]

	for tag, path in tagged:
		if tag == barlist_date_tag:
			try:
				with open(path, "r", encoding="utf-8") as f:
					return json.load(f)
			except Exception as exc:
				print(f"  ERROR: Metrics file {path} could not be read: {exc}",
				      file=sys.stderr)
				return {}

	available = sorted(set(t for t, _ in tagged))
	print(f"  ERROR: No same-day metrics for {fund_key} barlist date "
	      f"{barlist_date_tag}. Available: {', '.join(available)}",
	      file=sys.stderr)
	return {}


def load_fund_metrics(fund_key: str) -> dict[str, Any]:
	"""Load the canonical (latest) per-fund metrics file.

	Returns the flat metrics dict, or empty dict if missing.
	"""
	path = _metrics_file_for_fund(fund_key)
	if not os.path.exists(path):
		return {}
	with open(path, "r", encoding="utf-8") as f:
		return json.load(f)


def resolve_barlist_pdf(
	fund_key: str,
	local_override: str | None,
	url_override: str | None,
) -> tuple[str | None, dict[str, Any]]:
	"""Download bar-list PDF from the web with hash-based archival.

	If the existing bar list has different content than the newly downloaded
	one, the old file is archived with a date-stamped name before saving.
	"""
	fund_cfg = DEFAULT_FUNDS[fund_key]
	local_pdf = local_override or fund_cfg["local_pdf"]
	bar_url = url_override or fund_cfg.get("bar_list_url")

	meta = {
		"fund": fund_key,
		"display_name": fund_cfg["display_name"],
		"local_pdf": local_pdf,
		"bar_list_url": bar_url,
		"download_attempted": False,
		"download_status": None,
		"source": None,
		"archived_previous": None,
	}

	if not bar_url:
		meta["source"] = "no_url_configured"
		return None, meta

	# Read old file for hash comparison (before download overwrites it)
	old_data: bytes | None = None
	if os.path.exists(local_pdf):
		with open(local_pdf, "rb") as f:
			old_data = f.read()

	meta["download_attempted"] = True
	status = download_pdf(bar_url, local_pdf)
	meta["download_status"] = status
	meta["source"] = "downloaded"

	# Archive old bar list if content changed
	if old_data is not None:
		new_hash = _sha256_file(local_pdf)
		old_hash = _sha256(old_data)
		if old_hash != new_hash:
			archive = _archive_barlist(local_pdf, old_data)
			meta["archived_previous"] = archive
			print(f"  [barlist] New bar list detected for {fund_key}")
		else:
			print(f"  [barlist] Bar list unchanged for {fund_key}")
	else:
		print(f"  [barlist] First bar list download for {fund_key}")

	return local_pdf, meta


# ---------------------------------------------------------------------------
#  WisdomTree bar-list PDF parser
# ---------------------------------------------------------------------------
#  Row format (space-delimited text extracted by pdfplumber):
#    BAR_NUMBER REFINER_NAME... GROSS_WEIGHT FINE_WEIGHT ASSAY [YEAR] VAULT_NAME...
#
#  The weight cluster is the unique anchor — three consecutive numeric tokens:
#    gross  = digits with optional commas, exactly 3 decimals (e.g. 1,060.100)
#    fine   = same format (always 0.000 in this fund)
#    assay  = 0.NNNN  (e.g. 0.9999)
# ---------------------------------------------------------------------------

_WEIGHT_CLUSTER_RE = re.compile(
	r"(\d{1,3}(?:,\d{3})*\.\d{3})"   # gross weight
	r"\s+"
	r"(\d{1,3}(?:,\d{3})*\.\d{3})"   # fine weight
	r"\s+"
	r"(\d\.\d{4})"                    # assay / fineness
)

_YEAR_RE = re.compile(r"^\s*(\d{4})\s+(.+)$")  # kept for generic parser

# Lines that are headers / footers / metadata — never data rows
_SKIP_PATTERNS = re.compile(
	r"(?i)"
	r"bar\s+number|refiner\s+long|gross\s+weight|fine\s+weight|bar\s+assay|"
	r"vault\s+name|client\s+silver|stock\s+holdings|allocated\s+a/c|"
	r"total\s+allocated|end\s+of\s+silver|c\.o\.b|page\s+\d|"
	r"hbeu|law\s+debenture"
)

_COB_DATE_RE = re.compile(r"C\.O\.B[:\s]+(\d{1,2}\s+\w+\s+\d{4})")
_TOTAL_BAR_COUNT_RE = re.compile(r"Total\s+Allocated\s+Bar\s+Count[:\s]+(\d[\d,]*)")
_TOTAL_GROSS_RE = re.compile(r"Total\s+Allocated\s+Gross\s+Weight[:\s]+([\d,]+\.\d+)")


def _parse_wisdomtree_header(first_page_text: str) -> dict[str, Any]:
	"""Extract metadata from the first page header block."""
	meta: dict[str, Any] = {}

	m = _COB_DATE_RE.search(first_page_text)
	if m:
		meta["as_of_date"] = m.group(1)

	m = _TOTAL_BAR_COUNT_RE.search(first_page_text)
	if m:
		meta["declared_bar_count"] = int(m.group(1).replace(",", ""))

	m = _TOTAL_GROSS_RE.search(first_page_text)
	if m:
		meta["declared_total_gross_oz"] = float(m.group(1).replace(",", ""))

	return meta


def _split_serial_refiner(prefix: str) -> tuple[str, str | None]:
	"""Split the text before the weight cluster into (serial_number, refiner).

	Heuristic: starting from the right, consecutive tokens that do NOT contain
	digits form the refiner name.  Everything to their left is the serial number.
	This handles multi-part serials like '1E 452-11' followed by 'STATE REFINERIES'.
	"""
	tokens = prefix.split()
	if not tokens:
		return "", None
	if len(tokens) == 1:
		return tokens[0], None

	# Walk from the right — find the start of the refiner (tokens without digits)
	refiner_start = len(tokens)
	for i in range(len(tokens) - 1, -1, -1):
		if not re.search(r"\d", tokens[i]):
			refiner_start = i
		else:
			break

	if refiner_start == 0:
		# Every token is digit-free — treat the first token as serial, rest as refiner
		return tokens[0], " ".join(tokens[1:]) or None

	serial = " ".join(tokens[:refiner_start])
	refiner = " ".join(tokens[refiner_start:]) or None
	return serial, refiner


# Regex to match any leading number in the suffix after the weight cluster
_SUFFIX_NUM_RE = re.compile(r"^\s*(\d+)\s+(.+)$")


def _parse_wisdomtree_line(line: str, page_num: int) -> BarRecord | None:
	"""Parse a single WisdomTree bar-list data line using the weight cluster anchor."""
	if _SKIP_PATTERNS.search(line):
		return None

	match = _WEIGHT_CLUSTER_RE.search(line)
	if not match:
		return None

	# Everything before the weight cluster → bar_number + refiner
	prefix = line[: match.start()].strip()
	if not prefix:
		return None

	bar_number, refiner = _split_serial_refiner(prefix)
	if not bar_number:
		return None

	gross_oz = clean_number(match.group(1))
	fine_oz = clean_number(match.group(2))
	fineness = clean_number(match.group(3))

	# Everything after the weight cluster → optional year/ref number + vault
	suffix = line[match.end() :].strip()
	year: int | None = None
	vault: str | None = None

	if suffix:
		num_match = _SUFFIX_NUM_RE.match(suffix)
		if num_match:
			num_val = int(num_match.group(1))
			rest = num_match.group(2).strip()
			if 1900 <= num_val <= 2100:
				year = num_val
			# Strip the leading number regardless — either it's a year or a reference
			vault = rest or None
		else:
			vault = suffix

	return BarRecord(
		serial_number=bar_number,
		refiner=refiner,
		gross_oz=gross_oz,
		fine_oz=fine_oz,
		fineness=fineness,
		vault=vault,
		year=year,
		source_page=page_num,
		raw_line=line,
	)


def _detect_pdf_format(first_page_text: str) -> str:
	"""Detect if a PDF is WisdomTree, Invesco, or unknown format."""
	lower = first_page_text.lower()
	if "client silver stock holdings" in lower or "wisdomtree" in lower or "law debenture" in lower:
		return "wisdomtree"
	if "invesco" in lower or "jpmorgan" in lower:
		return "invesco"
	return "generic"


# ---------------------------------------------------------------------------
#  Invesco bar-list PDF parser
# ---------------------------------------------------------------------------
#  Row format (pdfplumber text extraction — pypdf breaks column alignment):
#    BRAND  BAR_NO  1000 oz  ASSAY  GROSS_OZ  FINE_OZ  VAULT
#
#  The "1000 oz" shape field is the unique anchor that separates the
#  brand/serial prefix from the numeric fields.
#
#  Examples:
#    Henan Yuguang Gold and Lead Company 20090117K7 1000 oz 9990 962.200 962.200 JPM London B (VLTB)
#    Russian State Refineries 11752 1000 oz 9999 942.100 942.100 JPM London B (VLTB)
#    Norddeutsche Affinerie AG N 60131 A 1000 oz 9990 862.600 862.600 JPM London B (VLTB)
# ---------------------------------------------------------------------------

_INVESCO_LINE_RE = re.compile(
	r"^(.+?)"                                   # brand + bar_no (non-greedy prefix)
	r"\s+1000\s+oz\s+"                           # shape anchor
	r"(\d{3,4})\s+"                              # assay (integer, e.g. 9990)
	r"(\d{1,3}(?:,\d{3})*\.\d{3})\s+"           # gross ounces
	r"(\d{1,3}(?:,\d{3})*\.\d{3})\s+"           # fine ounces
	r"(.+)$"                                     # vault
)

_INVESCO_SKIP_RE = re.compile(
	r"(?i)"
	r"^brand\s+bar|^running\s+total|^printed\s+on|^page\s+\d|"
	r"total\s+fto|total\s+bars|unit\s+of\s+weight|account\s+no|"
	r"commodity|value\s+date|bullion\s+weightlist|vault\s+copy|"
	r"jpmorgan\s+chase|incorporated|limited\s+liability|"
	r"bank\s+street|e14\s+5jp|london\s+branch|email|telex|tel\s*:|vat\s+reg"
)


def _parse_invesco_header(first_page_text: str) -> dict[str, Any]:
	"""Extract metadata from the Invesco PDF first page."""
	meta: dict[str, Any] = {}

	m = re.search(r"Total\s+Bars\s*:?\s*([\d,]+)", first_page_text)
	if m:
		meta["declared_bar_count"] = int(m.group(1).replace(",", ""))

	m = re.search(r"Total\s+FTO\s*:?\s*([\d,.]+)", first_page_text)
	if m:
		meta["declared_total_fine_oz"] = float(m.group(1).replace(",", ""))

	m = re.search(r"value\s+date\s+(\d{4}-\d{2}-\d{2})", first_page_text)
	if m:
		meta["as_of_date"] = m.group(1)

	return meta


def _split_invesco_brand_serial(prefix: str) -> tuple[str, str]:
	"""Split the text before '1000 oz' into (brand, serial_number).

	Brand is the company name (left side, multi-word, no digits).
	Serial starts at the rightmost digit-containing region, including
	adjacent single-letter tokens (e.g. 'N 60131 A').
	"""
	tokens = prefix.split()
	if not tokens:
		return "", ""
	if len(tokens) == 1:
		return "", tokens[0]

	# Scan from the right to find where the serial number begins
	serial_start = len(tokens)
	for i in range(len(tokens) - 1, -1, -1):
		if re.search(r"\d", tokens[i]):
			serial_start = i
		elif len(tokens[i]) <= 1:
			# Single character (letter or punctuation like '.') adjacent to
			# a digit token — part of the serial (e.g. "N 60131 A", "KPR 3841 .")
			if serial_start == i + 1:
				serial_start = i
			else:
				break
		else:
			break

	if serial_start == 0:
		# Could not separate — treat first token as brand
		return tokens[0], " ".join(tokens[1:])

	brand = " ".join(tokens[:serial_start])
	serial = " ".join(tokens[serial_start:])
	return brand, serial


def _parse_invesco_line(line: str, page_num: int) -> BarRecord | None:
	"""Parse a single Invesco bar-list data line using '1000 oz' as anchor.

	The data regex is specific enough to reject all header/footer/metadata
	lines, so no separate skip-pattern filter is needed.
	"""
	match = _INVESCO_LINE_RE.match(line)
	if not match:
		return None

	prefix = match.group(1).strip()
	if not prefix:
		return None

	brand, serial = _split_invesco_brand_serial(prefix)
	if not serial:
		return None

	assay_int = int(match.group(2))
	fineness = assay_int / 10_000.0   # 9990 → 0.9990
	gross_oz = clean_number(match.group(3))
	fine_oz = clean_number(match.group(4))
	vault = match.group(5).strip() or None

	return BarRecord(
		serial_number=serial,
		refiner=brand or None,
		gross_oz=gross_oz,
		fine_oz=fine_oz,
		fineness=fineness,
		vault=vault,
		year=None,
		source_page=page_num,
		raw_line=line,
	)


def parse_bars_from_pdf(pdf_path: str) -> tuple[list[BarRecord], dict[str, Any]]:
	bars: list[BarRecord] = []
	seen_keys: set[str] = set()
	parse_meta: dict[str, Any] = {
		"file": pdf_path,
		"format": "unknown",
		"pages": 0,
		"candidate_lines": 0,
		"accepted_rows": 0,
		"duplicates_skipped": 0,
		"header_metadata": {},
	}

	# Step 1: Use pdfplumber on page 1 to extract header metadata + detect format
	with pdfplumber.open(pdf_path) as pdf:
		if pdf.pages:
			first_text = pdf.pages[0].extract_text() or ""
		else:
			first_text = ""
	# pdfplumber is now closed — frees memory

	fmt = _detect_pdf_format(first_text)
	parse_meta["format"] = fmt

	if fmt == "wisdomtree":
		parse_meta["header_metadata"] = _parse_wisdomtree_header(first_text)
	elif fmt == "invesco":
		parse_meta["header_metadata"] = _parse_invesco_header(first_text)

	# Step 2: Extract text page-by-page
	# WisdomTree (952 pages): use pypdf for low memory
	# Invesco (208 pages): use pdfplumber for proper column alignment
	#   (pypdf breaks Invesco's multi-column layout into separate blocks)
	if fmt == "invesco":
		# pdfplumber gives correct line-aligned text for Invesco's table layout
		with pdfplumber.open(pdf_path) as pdf:
			parse_meta["pages"] = len(pdf.pages)
			for page_index, page in enumerate(pdf.pages, start=1):
				page_text = page.extract_text() or ""
				lines = [seg.strip() for seg in page_text.splitlines() if seg.strip()]
				for line in lines:
					parse_meta["candidate_lines"] += 1
					record = _parse_invesco_line(line, page_index)
					if record is None:
						continue
					dedup_key = line  # raw line uniqueness
					if dedup_key in seen_keys:
						parse_meta["duplicates_skipped"] += 1
						continue
					bars.append(record)
					seen_keys.add(dedup_key)
					parse_meta["accepted_rows"] += 1
	else:
		# Use pypdf for WisdomTree / generic (much lower memory for large PDFs)
		reader = pypdf.PdfReader(pdf_path)
		parse_meta["pages"] = len(reader.pages)

		for page_index, page in enumerate(reader.pages, start=1):
			page_text = page.extract_text() or ""
			lines = [seg.strip() for seg in page_text.splitlines() if seg.strip()]

			for line in lines:
				parse_meta["candidate_lines"] += 1

				if fmt == "wisdomtree":
					record = _parse_wisdomtree_line(line, page_index)
				else:
					record = _parse_generic_line(line, page_index)

				if record is None:
					continue

				if fmt == "wisdomtree":
					dedup_key = line
				else:
					dedup_key = f"{record.serial_number}|{record.refiner or ''}"

				if dedup_key in seen_keys:
					parse_meta["duplicates_skipped"] += 1
					continue

				bars.append(record)
				seen_keys.add(dedup_key)
				parse_meta["accepted_rows"] += 1

	bars.sort(key=lambda bar: (bar.serial_number, bar.refiner or "", bar.year or 0))
	return bars, parse_meta


# ---------------------------------------------------------------------------
#  Generic / fallback bar-list parser (for Invesco or unknown formats)
# ---------------------------------------------------------------------------

def _parse_generic_line(line: str, page_num: int) -> BarRecord | None:
	"""Fallback parser using heuristics — works for simple serial+weight rows."""
	text = re.sub(r"\s+", " ", line.strip())
	if not text:
		return None

	ignore_tokens = (
		"serial", "refiner", "gross", "fine", "fineness", "bar list",
		"page ", "invesco", "wisdomtree", "isin",
	)
	lower = text.lower()
	if any(tok in lower for tok in ignore_tokens):
		return None

	# Find a plausible serial number (first alphanumeric token with a digit)
	candidates = re.findall(r"[A-Z0-9][A-Z0-9\-/.]{3,}", text.upper())
	serial: str | None = None
	for token in candidates:
		if re.search(r"\d", token):
			serial = token
			break
	if serial is None:
		return None

	# Try the weight-cluster regex first for structured data
	m = _WEIGHT_CLUSTER_RE.search(line)
	if m:
		gross_oz = clean_number(m.group(1))
		fine_oz = clean_number(m.group(2))
		fineness = clean_number(m.group(3))
	else:
		# Fallback: find plausible weight values in 100-1200 oz range
		tokens = re.findall(r"\b\d{2,6}(?:[.,]\d{1,4})?\b", line)
		numeric_values: list[float] = []
		for tok in tokens:
			parsed = clean_number(tok)
			if parsed is not None and 100 <= parsed <= 1200:
				numeric_values.append(parsed)
		gross_oz = max(numeric_values) if numeric_values else None
		fine_oz = sorted(numeric_values)[-2] if len(numeric_values) >= 2 else None
		# Fineness
		fineness_candidates = re.findall(r"\b0\.\d{3,5}\b", line)
		fineness = None
		for v in fineness_candidates:
			p = clean_number(v)
			if p is not None and 0.85 <= p <= 1.0:
				fineness = p
				break

	return BarRecord(
		serial_number=serial,
		refiner=None,
		gross_oz=gross_oz,
		fine_oz=fine_oz,
		fineness=fineness,
		vault=None,
		year=None,
		source_page=page_num,
		raw_line=line,
	)


def aggregate_bars(bars: list[BarRecord]) -> dict[str, Any]:
	gross_values = [bar.gross_oz for bar in bars if bar.gross_oz is not None]
	fine_values = [bar.fine_oz for bar in bars if bar.fine_oz is not None and bar.fine_oz > 0]

	total_gross = sum(gross_values)
	total_fine = sum(fine_values)

	# If no explicit fine weights, compute from gross × fineness
	computed_fine = 0.0
	computed_fine_count = 0
	if total_fine == 0:
		for bar in bars:
			if bar.gross_oz is not None and bar.gross_oz > 0:
				fn = bar.fineness if bar.fineness and bar.fineness > 0 else 1.0
				computed_fine += bar.gross_oz * fn
				computed_fine_count += 1
		if computed_fine_count > 0:
			total_fine = computed_fine

	# Vault breakdown
	vault_counts: dict[str, int] = {}
	vault_gross: dict[str, float] = {}
	for bar in bars:
		v = bar.vault or "UNKNOWN"
		vault_counts[v] = vault_counts.get(v, 0) + 1
		if bar.gross_oz is not None:
			vault_gross[v] = vault_gross.get(v, 0.0) + bar.gross_oz

	# Refiner breakdown
	refiner_counts: dict[str, int] = {}
	for bar in bars:
		r = bar.refiner or "UNKNOWN"
		refiner_counts[r] = refiner_counts.get(r, 0) + 1

	return {
		"bar_count": len(bars),
		"bars_with_gross": len(gross_values),
		"bars_with_fine": len(fine_values),
		"total_gross_oz": total_gross,
		"total_fine_oz": total_fine,
		"fine_oz_computed_from_fineness": computed_fine_count > 0,
		"vaults": {v: {"bars": vault_counts[v], "gross_oz": vault_gross.get(v, 0.0)} for v in sorted(vault_counts)},
		"refiners": {r: refiner_counts[r] for r in sorted(refiner_counts)},
		"unique_vaults": len(vault_counts),
		"unique_refiners": len(refiner_counts),
	}


def compute_expected_oz(metrics: dict[str, Any]) -> tuple[float | None, str | None]:
	certs = metrics.get("certificates_outstanding")
	entitlement = metrics.get("entitlement_oz_per_certificate")
	assets = metrics.get("total_assets_usd")
	silver_price = metrics.get("silver_price_usd")

	if certs is not None and entitlement is not None:
		return float(certs) * float(entitlement), "certificates_x_entitlement"

	if assets is not None and silver_price and float(silver_price) > 0:
		return float(assets) / float(silver_price), "assets_div_silver_price"

	return None, None


def build_verification(
	aggregates: dict[str, Any],
	expected_oz: float | None,
	method: str | None,
	header_metadata: dict[str, Any] | None = None,
	fund_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
	# Use fine_oz if available, otherwise gross_oz
	physical_fine = aggregates.get("total_fine_oz", 0)
	physical_gross = aggregates.get("total_gross_oz", 0)
	physical_oz = physical_fine if physical_fine > 0 else physical_gross

	result: dict[str, Any] = {
		"expected_oz": expected_oz,
		"expected_method": method,
		"physical_oz_from_bar_list": physical_oz,
		"physical_gross_oz": physical_gross,
		"physical_fine_oz": physical_fine,
		"difference_oz": None,
		"difference_pct": None,
		"status": "insufficient_fund_metrics",
		"internal_consistency": None,
	}

	# Cross-check parsed totals against PDF header declared totals
	if header_metadata:
		declared_count = header_metadata.get("declared_bar_count")
		declared_gross = header_metadata.get("declared_total_gross_oz")
		declared_fine = header_metadata.get("declared_total_fine_oz")
		parsed_count = aggregates.get("bar_count", 0)

		consistency: dict[str, Any] = {}
		if declared_count is not None:
			consistency["declared_bar_count"] = declared_count
			consistency["parsed_bar_count"] = parsed_count
			consistency["bar_count_match"] = declared_count == parsed_count
		if declared_gross is not None:
			consistency["declared_total_gross_oz"] = declared_gross
			consistency["parsed_total_gross_oz"] = physical_gross
			consistency["gross_diff_oz"] = round(physical_gross - declared_gross, 3)
			consistency["gross_match"] = abs(physical_gross - declared_gross) < 0.01
		if declared_fine is not None:
			consistency["declared_total_fine_oz"] = declared_fine
			consistency["parsed_total_fine_oz"] = physical_fine
			consistency["fine_diff_oz"] = round(physical_fine - declared_fine, 3)
			consistency["fine_match"] = abs(physical_fine - declared_fine) < 0.01
		result["internal_consistency"] = consistency

	if expected_oz is None or expected_oz == 0:
		return result

	diff = physical_oz - expected_oz
	pct = (diff / expected_oz) * 100
	result["difference_oz"] = diff
	result["difference_pct"] = pct

	if abs(pct) <= 0.25:
		result["status"] = "match_within_0.25pct"
	elif abs(pct) <= 1.0:
		result["status"] = "warning_within_1pct"
	elif pct > 1.0:
		result["status"] = "overcollateralized_gt_1pct"
	else:
		result["status"] = "undercollateralized_gt_1pct"

	# Secondary check: compare against issuer-reported oz if available
	issuer_reported_oz = (fund_metrics or {}).get("wisdomtree_reported_oz")
	if issuer_reported_oz and issuer_reported_oz > 0:
		issuer_diff = physical_oz - issuer_reported_oz
		issuer_pct = (issuer_diff / issuer_reported_oz) * 100
		result["issuer_reported_oz"] = issuer_reported_oz
		result["issuer_difference_oz"] = issuer_diff
		result["issuer_difference_pct"] = issuer_pct

	return result


def load_metrics_file(path: str | None) -> dict[str, Any]:
	if not path:
		return {}
	if not os.path.exists(path):
		raise FileNotFoundError(f"metrics file not found: {path}")
	with open(path, "r", encoding="utf-8") as file_handle:
		return json.load(file_handle)


def write_json(path: str, payload: dict[str, Any]) -> None:
	with open(path, "w", encoding="utf-8") as file_handle:
		json.dump(payload, file_handle, indent=2)


def analyze_barlist(
	fund_key: str,
	pdf_path: str,
	metrics: dict[str, Any],
) -> dict[str, Any]:
	"""Parse a bar-list PDF and verify against fund metrics.

	This is the core analysis logic, usable for both the latest bar list
	and any historical PDFs.
	"""
	expected_oz, method = compute_expected_oz(metrics)

	output: dict[str, Any] = {
		"fund": fund_key,
		"display_name": DEFAULT_FUNDS[fund_key]["display_name"],
		"isin": DEFAULT_FUNDS[fund_key]["isin"],
		"ticker": DEFAULT_FUNDS[fund_key]["ticker"],
		"barlist_file": os.path.basename(pdf_path),
		"source": {"local_pdf": pdf_path, "source": "local_file"},
		"parse": None,
		"aggregates": None,
		"verification": None,
		"bars": [],
		"errors": [],
	}

	try:
		bars, parse_meta = parse_bars_from_pdf(pdf_path)
	except Exception as exc:
		output["errors"].append(f"parse_failed: {exc}")
		output["verification"] = build_verification(
			aggregates={"total_fine_oz": 0, "total_gross_oz": 0},
			expected_oz=expected_oz,
			method=method,
		)
		return output

	aggregates = aggregate_bars(bars)
	header_meta = parse_meta.get("header_metadata")
	verification = build_verification(aggregates, expected_oz, method, header_meta, metrics)

	output["parse"] = parse_meta
	output["aggregates"] = aggregates
	output["verification"] = verification
	output["bars"] = [bar.to_dict() for bar in bars]
	return output


def verify_fund(
	fund_key: str,
	local_pdf: str | None,
	url_override: str | None,
	metrics: dict[str, Any],
) -> dict[str, Any]:
	expected_oz, method = compute_expected_oz(metrics)

	resolved_pdf, source_meta = resolve_barlist_pdf(
		fund_key=fund_key,
		local_override=local_pdf,
		url_override=url_override,
	)

	if resolved_pdf is None:
		output: dict[str, Any] = {
			"fund": fund_key,
			"display_name": DEFAULT_FUNDS[fund_key]["display_name"],
			"isin": DEFAULT_FUNDS[fund_key]["isin"],
			"ticker": DEFAULT_FUNDS[fund_key]["ticker"],
			"barlist_file": None,
			"source": source_meta,
			"parse": None,
			"aggregates": None,
			"verification": build_verification(
				aggregates={"total_fine_oz": 0, "total_gross_oz": 0},
				expected_oz=expected_oz,
				method=method,
			),
			"bars": [],
			"errors": ["bar_list_pdf_not_available"],
		}
		return output

	result = analyze_barlist(fund_key, resolved_pdf, metrics)
	result["source"] = source_meta
	return result


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Verify silver ETC bar lists against fund metrics")
	parser.add_argument(
		"--metrics-dir",
		default=CACHE_DIR,
		help="Directory containing per-fund metrics files (etc_fund_metrics_<fund>.json)",
	)
	parser.add_argument(
		"--output-json",
		default=os.path.join(CACHE_DIR, "etc_silver_inventory_verification_latest.json"),
		help="Output JSON report path",
	)
	parser.add_argument("--invesco-pdf", default=None, help="Local override path for Invesco bar-list PDF")
	parser.add_argument("--wisdomtree-pdf", default=None, help="Local override path for WisdomTree bar-list PDF")
	parser.add_argument("--invesco-url", default=None, help="Override URL for Invesco bar list")
	parser.add_argument("--wisdomtree-url", default=None, help="Override URL for WisdomTree bar list")
	parser.add_argument(
		"--funds",
		nargs="+",
		choices=["invesco", "wisdomtree"],
		default=["invesco", "wisdomtree"],
		help="Which funds to process",
	)
	parser.add_argument(
		"--skip-documents",
		action="store_true",
		help="Skip downloading/syncing fund documents",
	)
	parser.add_argument(
		"--all-barlists",
		action="store_true",
		help="Also process all historical (archived) bar-list PDFs",
	)
	return parser.parse_args()


def _check_venv():
	"""Warn if running outside the project virtual environment."""
	import sys
	if not (hasattr(sys, "real_prefix") or
			(hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)):
		venv_path = os.path.join(os.path.dirname(__file__), ".venv")
		if os.path.isdir(venv_path):
			print("⚠  WARNING: Running with system Python — required packages")
			print("   (selenium, pypdf, pdfplumber, …) may be missing.")
			print(f"   Activate the venv first:  source {venv_path}/bin/activate")
			print()


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def _fmt_oz(val: Any) -> str:
	"""Format ounce values with comma separators and 3 decimal places."""
	if val is None:
		return "N/A"
	return f"{val:,.3f}"

def _fmt_pct(val: Any) -> str:
	"""Format percentage with sign."""
	if val is None:
		return "N/A"
	return f"{val:+.4f}%"

def _check_mark(ok: Any) -> str:
	if ok is None:
		return "—"
	return "✓" if ok else "✗"

def _print_summary_table(
	report: dict,
	doc_sync: dict | None = None,
	historical: dict | None = None,
	deltas: dict | None = None,
) -> str:
	"""Build and print a detailed summary table. Returns the text."""
	lines: list[str] = []
	_p = lines.append          # collect every line

	results = report.get("results", {})
	runtime = report.get("summary", {}).get("runtime_seconds", "?")

	W = 78
	_p("")
	_p("=" * W)
	_p("  ETC SILVER INVENTORY VERIFICATION REPORT")
	_p("=" * W)

	for fund_key, fund in results.items():
		ver = fund.get("verification") or {}
		ic = ver.get("internal_consistency") or {}
		hm = (fund.get("parse") or {}).get("header_metadata") or {}
		aggr = fund.get("aggregates") or {}
		parse = fund.get("parse") or {}
		errors = fund.get("errors", [])

		name = fund.get("display_name", fund_key)
		as_of = hm.get("as_of_date", "N/A")
		status = ver.get("status", "N/A")

		# Status display
		status_label = {
			"match_within_0.25pct": "MATCH",
			"warning_within_1pct": "WARNING",
			"overcollateralized_gt_1pct": "OVER-COLLATERALISED",
			"undercollateralized_gt_1pct": "UNDER-COLLATERALISED",
			"insufficient_fund_metrics": "NO METRICS",
		}.get(status, status.upper())

		_p("")
		_p(f"  {name}")
		_p(f"  Bar list date: {as_of}")
		_p("  " + "-" * (W - 2))

		# --- Internal consistency (header vs parsed) ---
		_p("  PDF Header vs Parsed Bars:")
		rows: list[tuple[str, str, str, str]] = []

		dc = ic.get("declared_bar_count")
		pc = ic.get("parsed_bar_count")
		if dc is not None:
			rows.append(("Bar count", f"{dc:,}", f"{pc:,}", _check_mark(ic.get("bar_count_match"))))

		dg = ic.get("declared_total_gross_oz")
		pg = ic.get("parsed_total_gross_oz")
		if dg is not None:
			rows.append(("Gross oz", _fmt_oz(dg), _fmt_oz(pg), _check_mark(ic.get("gross_match"))))

		df = ic.get("declared_total_fine_oz")
		pf = ic.get("parsed_total_fine_oz")
		if df is not None:
			rows.append(("Fine oz", _fmt_oz(df), _fmt_oz(pf), _check_mark(ic.get("fine_match"))))

		if rows:
			_p(f"    {'Metric':<12} {'Header':>18} {'Parsed':>18} Match")
			_p(f"    {'-'*12} {'-'*18} {'-'*18} -----")
			for label, hdr_val, par_val, chk in rows:
				_p(f"    {label:<12} {hdr_val:>18} {par_val:>18}   {chk}")
		else:
			_p("    (no header totals available)")

		# --- Verification against fund metrics ---
		_p("")
		_p("  Fund Metrics vs Physical Silver:")
		expected = ver.get("expected_oz")
		physical = ver.get("physical_oz_from_bar_list")
		diff_oz = ver.get("difference_oz")
		diff_pct = ver.get("difference_pct")
		method = ver.get("expected_method", "N/A")

		_p(f"    {'Expected oz':<22} {_fmt_oz(expected):>18}  ({method})")
		_p(f"    {'Physical oz':<22} {_fmt_oz(physical):>18}")
		_p(f"    {'Difference oz':<22} {_fmt_oz(diff_oz):>18}")
		_p(f"    {'Difference %':<22} {_fmt_pct(diff_pct):>18}")
		_p(f"    {'Status':<22} {status_label:>18}")

		# --- Parsing stats ---
		accepted = parse.get("accepted_rows", 0)
		rejected = parse.get("rejected_rows", 0)
		dupes = parse.get("duplicates_removed", 0)
		vaults = list((aggr.get("by_vault") or {}).keys())

		_p("")
		_p("  Parsing Stats:")
		_p(f"    {'Accepted bars':<22} {accepted:>18,}")
		if rejected:
			_p(f"    {'Rejected rows':<22} {rejected:>18,}")
		if dupes:
			_p(f"    {'Duplicates removed':<22} {dupes:>18,}")
		if vaults:
			_p(f"    {'Vaults':<22} {len(vaults):>18}")
			for v in vaults:
				vault_data = aggr["by_vault"][v]
				cnt = vault_data.get("bar_count", 0)
				_p(f"      - {v}: {cnt:,} bars")

		if errors:
			_p("")
			_p("  Errors:")
			for e in errors:
				_p(f"    ⚠ {e}")

	# --- Historical bar-list results ---
	if historical:
		_status_map = {
			"match_within_0.25pct": "MATCH",
			"warning_within_1pct": "WARNING",
			"overcollateralized_gt_1pct": "OVER-COLLAT",
			"undercollateralized_gt_1pct": "UNDER-COLLAT",
			"insufficient_fund_metrics": "NO METRICS",
		}
		_p("")
		_p("  " + "-" * (W - 2))
		_p("  Historical Bar Lists:")
		_p(f"    {'File':<36} {'BL Date':<12} {'Metrics':<10}"
		   f" {'Bars':>7} {'Status'}")
		_p(f"    {'-'*36} {'-'*12} {'-'*10} {'-'*7} {'-'*13}")
		for fund_key, entries in historical.items():
			for entry in entries:
				hm = (entry.get("parse") or {}).get("header_metadata") or {}
				ver = entry.get("verification") or {}
				bfile = entry.get("barlist_file", "?")
				as_of = hm.get("as_of_date", "N/A")
				m_date = entry.get("metrics_date", "N/A")
				bars_n = entry.get("bar_count", 0)
				st_raw = ver.get("status", "N/A")
				st_lbl = _status_map.get(st_raw, st_raw.upper()[:13])
				_p(f"    {bfile:<36} {as_of:<12} {m_date:<10}"
				   f" {bars_n:>7,} {st_lbl}")
		_p("")

	# --- Vault delta summary ---
	if deltas:
		from vault_delta import format_delta_summary_lines
		lines.extend(format_delta_summary_lines(deltas))

	# --- Document sync summary ---
	if doc_sync:
		_p("")
		_p("  " + "-" * (W - 2))
		_p("  Document Sync Summary:")
		_p(f"    {'Provider':<22} {'New':>6} {'Upd':>6} {'Same':>6}"
		   f" {'Fail':>6} {'Total':>6}")
		_p(f"    {'-'*22} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6}")
		tot_n = tot_u = tot_s = tot_f = tot_t = 0
		for prov, st in doc_sync.items():
			_p(f"    {prov.title():<22} {st.new:>6} {st.updated:>6}"
			   f" {st.unchanged:>6} {st.failed:>6} {st.total:>6}")
			tot_n += st.new
			tot_u += st.updated
			tot_s += st.unchanged
			tot_f += st.failed
			tot_t += st.total
		if len(doc_sync) > 1:
			_p(f"    {'Total':<22} {tot_n:>6} {tot_u:>6}"
			   f" {tot_s:>6} {tot_f:>6} {tot_t:>6}")
	elif doc_sync is None:
		pass  # --skip-documents used

	_p("")
	_p("=" * W)
	_p(f"  Runtime: {runtime}s")
	_p("=" * W)
	_p("")

	# Print and return the full text
	text = "\n".join(lines)
	print(text)
	return text


def main() -> int:
	_check_venv()
	ensure_cache_dir()
	args = parse_args()

	started = time.time()

	# Sync fund documents (factsheets, audit reports, etc.)
	doc_sync_results = None
	if not args.skip_documents:
		try:
			from download_documents import sync_all_documents
			doc_sync_results = sync_all_documents(verbose=True)
		except Exception as e:
			print(f"\n  WARNING: Document sync failed: {e}")

	report: dict[str, Any] = {
		"generated_utc": now_iso(),
		"script": "verify_silver_etcs.py",
		"funds_requested": args.funds,
		"inputs": {
			"metrics_dir": args.metrics_dir,
		},
		"results": {},
		"summary": {},
	}

	for fund in args.funds:
		metrics_for_fund = load_fund_metrics(fund)
		if not metrics_for_fund:
			print(f"  ERROR: No metrics file for fund '{fund}' in "
			      f"{args.metrics_dir} — verification will have "
			      f"status=insufficient_fund_metrics", file=sys.stderr)
			print(f"  Run 'python fetch_{fund}.py --update-metrics' first, or "
			      f"use run_all.py which does this automatically.", file=sys.stderr)
			return 1
		result = verify_fund(
			fund_key=fund,
			local_pdf=args.invesco_pdf if fund == "invesco" else args.wisdomtree_pdf,
			url_override=args.invesco_url if fund == "invesco" else args.wisdomtree_url,
			metrics=metrics_for_fund,
		)
		report["results"][fund] = result

	# --- Vault delta analysis — track bar adds/removes/re-entries ---
	from vault_delta import update_bar_history
	delta_results: dict[str, dict[str, Any]] = {}
	print("\n  Running vault delta analysis ...")
	for fund in args.funds:
		fund_result = report["results"].get(fund, {})
		bars = fund_result.get("bars", [])
		if not bars:
			print(f"    {fund}: no bars parsed — skipping delta")
			continue

		# Extract the as-of date from the bar list header
		hm = (fund_result.get("parse") or {}).get("header_metadata") or {}
		as_of = hm.get("as_of_date", "")
		date_tag = _normalise_date_tag(as_of) if as_of else datetime.now().strftime("%Y%m%d")

		delta = update_bar_history(fund, bars, date_tag)
		delta_results[fund] = delta

		n_add = len(delta.get("added", []))
		n_rem = len(delta.get("removed", []))
		n_ret = len(delta.get("returned", []))
		n_re = len(delta.get("re_entered", []))
		if delta.get("is_first_snapshot"):
			print(f"    {fund}: first snapshot — {len(bars):,} bars recorded")
		elif delta.get("is_repeat"):
			print(f"    {fund}: same snapshot date ({date_tag}) — no delta")
		else:
			print(f"    {fund}: +{n_add:,} added, -{n_rem:,} removed,"
				  f" {n_ret:,} returned, {n_re:,} re-entry flags")

	# --- Historical bar-list analysis (if requested) ---
	report["historical"] = {}
	if args.all_barlists:
		print("\n  Analysing historical bar lists ...")
		for fund in args.funds:
			metrics_files = find_all_metrics_files_for_fund(fund)
			print(f"    {fund}: found {len(metrics_files)} metrics file(s)")
			canonical = DEFAULT_FUNDS[fund]["local_pdf"]
			all_pdfs = find_all_barlists(fund)
			# Exclude the canonical file (already processed above)
			historical_pdfs = [p for p in all_pdfs if p != canonical]
			if not historical_pdfs:
				print(f"    {fund}: no archived bar lists found")
				continue
			print(f"    {fund}: found {len(historical_pdfs)} archived bar list(s)")
			fund_history: list[dict[str, Any]] = []
			for pdf_path in historical_pdfs:
				# Extract the date tag from the bar-list filename
				bl_base = os.path.basename(pdf_path)
				bl_match = re.search(r"_(\d{8})", bl_base)
				bl_date_tag = bl_match.group(1) if bl_match else "99999999"

				# Find the same-day metrics for this fund + bar-list date
				hist_fund_metrics = find_metrics_for_fund(fund, bl_date_tag)
				if not hist_fund_metrics:
					print(f"    ERROR: No metrics for {fund} on {bl_date_tag} "
					      f"— historical analysis will be incomplete",
					      file=sys.stderr)
				metrics_tag = _metrics_date_tag(
					next((f for f in metrics_files
						  if _metrics_date_tag(f).replace("-", "") <= bl_date_tag),
						 metrics_files[-1] if metrics_files else "")
				) if metrics_files else "none"

				print(f"    Parsing {bl_base} (metrics: {metrics_tag}) ...")
				result = analyze_barlist(fund, pdf_path, hist_fund_metrics)
				# Don't store full bar list in JSON (very large)
				result_lite = {k: v for k, v in result.items() if k != "bars"}
				result_lite["bar_count"] = len(result.get("bars", []))
				result_lite["metrics_date"] = metrics_tag
				fund_history.append(result_lite)
			report["historical"][fund] = fund_history

	summary_rows = []
	for fund in report["results"].values():
		aggr = fund.get("aggregates") or {}
		ver = fund.get("verification") or {}
		summary_rows.append(
			{
				"fund": fund["fund"],
				"display_name": fund["display_name"],
				"bar_count": aggr.get("bar_count", 0),
				"physical_oz": ver.get("physical_oz_from_bar_list"),
				"expected_oz": ver.get("expected_oz"),
				"difference_pct": ver.get("difference_pct"),
				"status": ver.get("status"),
				"errors": fund.get("errors", []),
			}
		)

	report["summary"] = {
		"funds_processed": len(summary_rows),
		"rows": summary_rows,
		"runtime_seconds": round(time.time() - started, 2),
	}

	write_json(args.output_json, report)

	# Also save a date-stamped copy for historical comparison
	date_tag = datetime.now().strftime("%Y%m%d")
	dated_path = os.path.join(
		CACHE_DIR, f"etc_silver_inventory_verification_{date_tag}.json"
	)
	write_json(dated_path, report)

	summary_text = _print_summary_table(
		report, doc_sync=doc_sync_results,
		historical=report.get("historical"),
		deltas=delta_results if delta_results else None,
	)

	# Write .txt report with date+time in filename
	time_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
	txt_path = os.path.join(
		CACHE_DIR, f"verification_report_{time_tag}.txt",
	)
	with open(txt_path, "w", encoding="utf-8") as fh:
		fh.write(summary_text.lstrip("\n") + "\n")

	# Write vault delta report(s)
	delta_paths: list[str] = []
	if delta_results:
		from vault_delta import format_delta_report
		for fund_key, delta in delta_results.items():
			delta_text = format_delta_report(fund_key, delta)
			delta_file = os.path.join(
				CACHE_DIR, f"vault_delta_{fund_key}_{time_tag}.txt",
			)
			with open(delta_file, "w", encoding="utf-8") as fh:
				fh.write(delta_text + "\n")
			delta_paths.append(delta_file)

	print(f"\nSaved JSON report:  {args.output_json}")
	print(f"Saved dated copy:   {dated_path}")
	print(f"Saved text report:  {txt_path}")
	for dp in delta_paths:
		print(f"Saved delta report: {dp}")

	# Regenerate time-series CSV
	try:
		from generate_csv import generate_csv
		csv_path, csv_errors = generate_csv(funds=args.funds)
		print(f"Saved CSV report:   {csv_path}")
		if csv_errors:
			print(f"\n  ERROR: CSV generation had {csv_errors} data correlation "
			      f"error(s)", file=sys.stderr)
			return 1
	except Exception as exc:
		print(f"\n  ERROR: CSV generation failed: {exc}", file=sys.stderr)
		return 1

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
