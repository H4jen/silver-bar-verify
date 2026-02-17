# Copilot Instructions

## Git Rules

- **NEVER run `git push` without explicit user consent.** Always ask before pushing.
- `git add` and `git commit` are fine without asking, but always confirm before push.

## SSH Rules

- When connecting to the remote server (192.168.66.77), **always use `ssh yspy-server`** (the SSH config alias), never `ssh hajen@192.168.66.77` directly. The alias uses key-based auth and requires no password.
