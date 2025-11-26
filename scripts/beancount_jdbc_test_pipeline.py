#!/usr/bin/env python3
"""
Unified CLI for Beancount JDBC testing pipeline utilities.

Current subcommands:
    find    Discover public .beancount ledger files on GitHub (replacement for scripts/find_ledgers.sh).
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Sequence, Tuple

SEARCH_API = "https://api.github.com/search/code"
DEFAULT_QUERY = "extension:beancount"
DEFAULT_PER_PAGE = 100
DEFAULT_MAX_PAGES = 10
USER_AGENT = "beancount-jdbc-test-pipeline/0.1"


class GitHubRequestError(Exception):
    """Raised when a GitHub API request fails."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def load_token(token_arg: Optional[str]) -> Optional[str]:
    """Load a GitHub token from CLI arg, environment, or token file."""
    env_token = token_arg or os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if env_token:
        return env_token.strip()

# Support legacy token file location used by shell scripts.
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    token_candidates = [
        script_dir / ".github_token",
        repo_root / ".github_token",
        script_dir / "scripts" / ".github_token",
    ]
    for path in token_candidates:
        if path.is_file():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                return content
    return None


def build_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def http_get(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, str]] = None,
) -> Tuple[int, bytes, Dict[str, str]]:
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request) as response:
            status = response.getcode()
            body = response.read()
            resp_headers = dict(response.headers)
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        status = exc.code
        body = exc.read()
        resp_headers = dict(exc.headers)
    return status, body, {k.lower(): v for k, v in resp_headers.items()}


def parse_json(body: bytes) -> Dict[str, object]:
    try:
        return json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise GitHubRequestError(0, f"Failed to parse JSON response: {exc}") from exc


def extract_include_targets(decoded_text: str) -> List[str]:
    """Parse include directives from decoded ledger content."""
    include_regex = re.compile(r'^\s*include\s+"([^"]+)"\s*$', re.IGNORECASE)
    targets: List[str] = []
    seen: set[str] = set()
    for line in decoded_text.splitlines():
        if re.match(r"^\s*[;#]", line):
            continue
        match = include_regex.match(line)
        if match:
            target = match.group(1).strip()
            if target and target not in seen:
                seen.add(target)
                targets.append(target)
    return targets


def fetch_contents_metadata(
    contents_url: str,
    headers: Dict[str, str],
    check_include: bool,
) -> Tuple[Optional[str], Optional[str], List[str]]:
    status, body, _ = http_get(contents_url, headers=headers)
    if status != 200:
        return None, None, []

    payload = parse_json(body)
    size_val = payload.get("size")
    size_str: Optional[str]
    if isinstance(size_val, int):
        size_str = str(size_val)
    else:
        size_str = None

    include_flag: Optional[str] = None
    include_targets: List[str] = []

    if check_include:
        content_b64 = payload.get("content")
        encoding = payload.get("encoding")
        if isinstance(content_b64, str) and encoding == "base64":
            try:
                decoded_bytes = base64.b64decode(content_b64, validate=False)
                decoded_text = decoded_bytes.decode("utf-8", errors="replace")
                include_targets = extract_include_targets(decoded_text)
                include_flag = "yes" if include_targets else "no"
            except Exception:
                include_flag = "unknown"
        else:
            include_flag = "unknown"

    return size_str, include_flag, include_targets


def maybe_wait_rate_limit(rate_remaining: Optional[str], rate_reset: Optional[str]) -> None:
    if rate_remaining is None or rate_reset is None:
        return
    try:
        remaining_int = int(rate_remaining)
        reset_epoch = int(rate_reset)
    except ValueError:
        return

    if remaining_int > 1:
        return

    now_epoch = int(time.time())
    wait_seconds = reset_epoch - now_epoch + 2
    if wait_seconds > 0:
        print(f"Rate limit nearly exhausted; sleeping for {wait_seconds}s until reset.", file=sys.stderr)
        time.sleep(wait_seconds)


def emit_record(
    handle,
    raw_url: str,
    include_size: bool,
    check_include: bool,
    size_val: Optional[str],
    include_flag: Optional[str],
    include_targets: Sequence[str],
) -> None:
    include_targets_csv = ",".join(include_targets) if include_targets else ""

    if include_size and check_include:
        handle.write(f"{raw_url}\t{size_val or 'unknown'}\t{include_flag or 'unknown'}\t{include_targets_csv}\n")
    elif include_size:
        handle.write(f"{raw_url}\t{size_val or 'unknown'}\n")
    elif check_include:
        handle.write(f"{raw_url}\t{include_flag or 'unknown'}\t{include_targets_csv}\n")
    else:
        handle.write(f"{raw_url}\n")


def parse_targets_csv(raw: str) -> List[str]:
    if not raw:
        return []
    parts = []
    for piece in raw.split(","):
        trimmed = piece.strip()
        if trimmed:
            parts.append(trimmed)
    return parts


def parse_filtered_records(path: Path) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            raw_url = parts[0].strip()
            size = parts[1].strip()
            type_label = parts[2].strip().lower()
            include_targets_raw = parts[3].strip() if len(parts) >= 4 else ""

            prefix = "https://raw.githubusercontent.com/"
            if not raw_url.startswith(prefix):
                continue
            rel = raw_url[len(prefix) :]
            rel_parts = rel.split("/", 3)
            if len(rel_parts) < 4:
                continue
            owner, repo, sha, path_part = rel_parts
            base_dir = path_part.rsplit("/", 1)[0] if "/" in path_part else "."

            records.append(
                {
                    "raw_url": raw_url,
                    "size": size,
                    "type": type_label,
                    "include_targets": parse_targets_csv(include_targets_raw),
                    "owner": owner,
                    "repo": repo,
                    "sha": sha,
                    "path": path_part,
                    "base_dir": base_dir,
                    "basename": os.path.basename(path_part),
                }
            )
    return records


def resolve_include_path(base_dir: str, target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    if base_dir in ("", "."):
        return target
    return f"{base_dir.rstrip('/')}/{target}"


def size_is_too_small(size_str: str, threshold: int) -> bool:
    try:
        size_int = int(size_str)
    except (TypeError, ValueError):
        return False  # Unknown size; do not treat as too small.
    return size_int <= threshold


def read_source_urls(source_file: Path, allowed_types: Optional[set[str]] = None) -> Tuple[List[str], int]:
    urls: List[str] = []
    skipped_by_type = 0
    if not source_file.is_file():
        raise GitHubRequestError(0, f"Source list not found: {source_file}")
    with source_file.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw_line = line.rstrip("\n")
            if not raw_line or raw_line.startswith("#"):
                continue
            parts = raw_line.split("\t")
            url = parts[0].strip()
            ledger_type = parts[2].strip().lower() if len(parts) >= 3 else ""
            if allowed_types and ledger_type and ledger_type not in allowed_types:
                skipped_by_type += 1
                continue
            if re.match(r"^(https?|ftp)://", url):
                urls.append(url.rstrip("\r"))
    return urls, skipped_by_type


def slug_for_url(url: str, base: str) -> str:
    trimmed = re.sub(r"^[a-zA-Z]+://", "", url)
    host, _, rest = trimmed.partition("/")
    if host == "raw.githubusercontent.com":
        owner, _, rest = rest.partition("/")
        repo, _, _ = rest.partition("/")
        if owner and repo:
            slug = f"{owner}-{repo}-{base}"
            slug = re.sub(r"[^A-Za-z0-9_.-]", "-", slug).lower()
            slug = re.sub(r"-+", "-", slug).strip("-")
            return slug
    slug = trimmed[:-len(base)] if trimmed.endswith(base) else trimmed
    slug = slug.replace("/", "-")
    slug = re.sub(r"[^A-Za-z0-9_.-]", "-", slug).lower()
    slug = re.sub(r"-+", "-", slug).strip("-")
    return f"{slug}-{base}"


def normalize_url(url: str) -> str:
    """Percent-encode non-ASCII path/query segments so urllib can handle them."""
    try:
        parsed = urllib.parse.urlsplit(url)
    except Exception:
        return url
    if not parsed.scheme or not parsed.netloc:
        return url
    path = urllib.parse.quote(parsed.path, safe="/%")
    query = urllib.parse.quote(parsed.query, safe="=&%")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, parsed.fragment))


def sanitize_include_relpath(path_str: str) -> str:
    # Strip leading slashes and parent refs to keep includes sandboxed under the subfolder.
    from pathlib import PurePosixPath

    safe_parts = [p for p in PurePosixPath(path_str).parts if p not in ("", ".", "..")]
    if not safe_parts:
        safe_parts = [PurePosixPath(path_str).name or "include.beancount"]
    return str(PurePosixPath(*safe_parts))


def run_sql_to_sqlite(python_exe: Path, sql_script: Path, ledger_path: Path, target_path: Path, debug: bool) -> None:
    if not sql_script.is_file():
        raise GitHubRequestError(0, f"SQL script not found: {sql_script}")
    if not python_exe.is_file():
        raise GitHubRequestError(0, f"Python interpreter not found: {python_exe}")

    env = os.environ.copy()
    env.setdefault("BEANCOUNT_FLAGS", "--no-cache")
    cmd = [
        str(python_exe),
        "-c",
        (
            "import runpy, logging, sys;"
            "script, ledger, target, debug_flag = sys.argv[1:5];"
            "debug = debug_flag == '1';"
            "import pathlib;"
            "pkg_root = pathlib.Path(script).parents[2];"
            "sys.path.insert(0, str(pkg_root));"
            "import beancount;"  # noqa: F401
            "ctx = runpy.run_path(script);"
            "command = ctx.get('main');"
            "import os;"
            "os.environ.setdefault('BEANCOUNT_FLAGS', '--no-cache');"
            "import logging;"
            "logging.basicConfig(level=logging.WARNING if not debug else logging.INFO, format='%(levelname)s: %(message)s');"
            "command.main(args=[ledger, target], prog_name='bean-sql', standalone_mode=False)"
        ),
        str(sql_script),
        str(ledger_path),
        str(target_path),
        "1" if debug else "0",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise GitHubRequestError(proc.returncode, f"bean-sql failed: {proc.stderr or proc.stdout}")


def load_meta(meta_path: Path) -> Dict[str, Tuple[str, str]]:
    meta: Dict[str, Tuple[str, str]] = {}
    if meta_path.is_file():
        with meta_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) >= 3:
                    url, etag, last_mod = parts[0], parts[1], parts[2]
                    meta[url] = (etag, last_mod)
    return meta


def write_meta(meta_path: Path, meta: Dict[str, Tuple[str, str]]) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for url, (etag, last_mod) in meta.items():
            fh.write(f"{url}|{etag}|{last_mod}\n")
    tmp.replace(meta_path)


def fetch_url(url: str, etag: str, last_mod: str) -> Tuple[int, bytes, Dict[str, str]]:
    url = normalize_url(url)
    headers = {
        "User-Agent": USER_AGENT,
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_mod:
        headers["If-Modified-Since"] = last_mod
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request) as response:
            status = response.getcode()
            body = response.read()
            resp_headers = dict(response.headers)
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        status = exc.code
        body = exc.read()
        resp_headers = dict(exc.headers)
    return status, body, {k.lower(): v for k, v in resp_headers.items()}


def ensure_valid_int(value: int, name: str, *, minimum: int = 1, maximum: Optional[int] = None) -> None:
    if value < minimum or (maximum is not None and value > maximum):
        max_msg = f" and <= {maximum}" if maximum is not None else ""
        raise argparse.ArgumentTypeError(f"{name} must be >= {minimum}{max_msg}")


def find_ledgers(args: argparse.Namespace) -> None:
    ensure_valid_int(args.per_page, "per_page", minimum=1, maximum=100)
    ensure_valid_int(args.max_pages, "max_pages", minimum=1)

    token = load_token(args.token)
    headers = build_headers(token)
    if not token:
        print(
            "Note: using unauthenticated GitHub requests (low rate limit). Set GITHUB_TOKEN for better coverage.",
            file=sys.stderr,
        )

    if args.output and args.output != "-":
        output_handle = open(args.output, "w", encoding="utf-8")
        close_output = True
    else:
        output_handle = sys.stdout
        close_output = False

    seen: set[str] = set()
    total = 0
    page = 1
    try:
        while page <= args.max_pages:
            print(f"Fetching page {page} ...", file=sys.stderr)
            status, body, resp_headers = http_get(
                SEARCH_API,
                headers=headers,
                params={
                    "q": args.query,
                    "per_page": str(args.per_page),
                    "page": str(page),
                },
            )

            if status != 200:
                message = ""
                try:
                    message = parse_json(body).get("message", "")  # type: ignore[assignment]
                except Exception:
                    message = body.decode("utf-8", errors="replace").strip()
                raise GitHubRequestError(status, f"GitHub API returned HTTP {status}{' - ' + message if message else ''}")

            payload = parse_json(body)
            items = payload.get("items")
            if not isinstance(items, list):
                raise GitHubRequestError(status, f"Unexpected response structure on page {page}")

            if not items:
                print(f"No more results after page {page - 1}.", file=sys.stderr)
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                repo_info = item.get("repository") or {}
                if not isinstance(repo_info, dict):
                    continue
                repo_full = repo_info.get("full_name")
                path = item.get("path")
                sha = item.get("sha")
                contents_url = item.get("url")
                if not (isinstance(repo_full, str) and isinstance(path, str) and isinstance(sha, str)):
                    continue
                key = f"{repo_full}/{path}"
                if key in seen:
                    continue
                seen.add(key)
                # Prefer download_url from the contents API; fall back to default-branch raw URL.
                raw_url: Optional[str] = None
                download_url: Optional[str] = None
                default_branch = repo_info.get("default_branch")
                if isinstance(default_branch, str):
                    raw_url = f"https://raw.githubusercontent.com/{repo_full}/{default_branch}/{path}"

                size_val: Optional[str] = None
                include_flag: Optional[str] = None
                include_targets: List[str] = []
                if (args.include_size or args.check_include) and isinstance(contents_url, str):
                    size_val, include_flag, include_targets = fetch_contents_metadata(
                        contents_url, headers=headers, check_include=args.check_include
                    )
                    if raw_url is None:
                        try:
                            status_meta, body_meta, _ = http_get(contents_url, headers=headers)
                            if status_meta == 200:
                                content_payload = parse_json(body_meta)
                                download_url_val = content_payload.get("download_url")
                                if isinstance(download_url_val, str):
                                    raw_url = download_url_val
                        except Exception:
                            raw_url = raw_url
                elif isinstance(contents_url, str):
                    # Even when not fetching size/include, try to pull download_url in a lightweight call.
                    try:
                        status_meta, body_meta, _ = http_get(contents_url, headers=headers)
                        if status_meta == 200:
                            content_payload = parse_json(body_meta)
                            download_url_val = content_payload.get("download_url")
                            if isinstance(download_url_val, str):
                                raw_url = download_url_val
                    except Exception:
                        raw_url = raw_url
                emit_record(
                    output_handle,
                    raw_url=raw_url or "",
                    include_size=args.include_size,
                    check_include=args.check_include,
                    size_val=size_val,
                    include_flag=include_flag,
                    include_targets=include_targets,
                )
                total += 1

            maybe_wait_rate_limit(resp_headers.get("x-ratelimit-remaining"), resp_headers.get("x-ratelimit-reset"))

            if len(items) < args.per_page:
                print(f"Last page reached with {len(items)} results.", file=sys.stderr)
                break
            page += 1
    finally:
        if close_output:
            output_handle.close()

    print(f"Discovered {total} unique ledger URLs.", file=sys.stderr)


def filter_ledgers(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    if not input_path.is_file():
        raise GitHubRequestError(0, f"Input not found: {input_path}")

    if args.output and args.output != "-":
        out_handle = open(args.output, "w", encoding="utf-8")
        close_output = True
    else:
        out_handle = sys.stdout
        close_output = False

    records: Dict[str, Dict[str, object]] = {}
    input_rows = 0
    dedup_dropped = 0
    main_rows = 0

    with input_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            input_rows += 1
            raw_url = parts[0].strip()
            size = parts[1].strip() if len(parts) >= 2 else ""
            include_flag = parts[2].strip().lower() if len(parts) >= 3 else ""
            include_targets_raw = parts[3].strip() if len(parts) >= 4 else ""

            prefix = "https://raw.githubusercontent.com/"
            if not raw_url.startswith(prefix):
                continue
            rel = raw_url[len(prefix) :]
            rel_parts = rel.split("/", 3)
            if len(rel_parts) < 4:
                continue
            owner, repo, sha, path = rel_parts
            base_dir = path.rsplit("/", 1)[0] if "/" in path else "."

            resolved_targets = [
                resolve_include_path(base_dir, target) for target in parse_targets_csv(include_targets_raw)
            ]

            key = f"{os.path.basename(path)}|{size}"
            if key not in records:
                records[key] = {
                    "raw_url": raw_url,
                    "size": size,
                    "include_flag": include_flag,
                    "include_targets": set(resolved_targets),
                    "owner": owner,
                    "repo": repo,
                    "sha": sha,
                    "path": path,
                    "base_dir": base_dir,
                }
                if include_flag == "yes":
                    main_rows += 1
            else:
                rec = records[key]
                if include_flag == "yes":
                    rec["include_flag"] = "yes"
                    main_rows += 1
                rec_targets = rec["include_targets"]
                if isinstance(rec_targets, set):
                    rec_targets.update(resolved_targets)
                dedup_dropped += 1

    include_target_keys: set[tuple[str, str, str, str]] = set()
    for rec in records.values():
        if rec.get("include_flag") == "yes":
            base_dir = rec["base_dir"]
            if not isinstance(base_dir, str):
                continue
            owner = rec.get("owner")
            repo = rec.get("repo")
            sha = rec.get("sha")
            if not all(isinstance(x, str) for x in (owner, repo, sha)):
                continue
            targets = rec.get("include_targets", set())
            if isinstance(targets, set):
                for tgt in targets:
                    include_target_keys.add((owner, repo, sha, tgt))

    kept = 0
    kept_main = 0
    kept_single = 0
    include_targets_skipped = 0
    too_small = 0
    try:
        for rec in records.values():
            owner = rec.get("owner")
            repo = rec.get("repo")
            sha = rec.get("sha")
            path = rec.get("path")
            if not all(isinstance(x, str) for x in (owner, repo, sha, path)):
                continue

            if (owner, repo, sha, path) in include_target_keys:
                include_targets_skipped += 1
                continue  # Skip include targets; fetched later.

            include_flag = rec.get("include_flag")
            size = rec.get("size")
            include_targets = rec.get("include_targets", set())

            if include_flag == "yes":
                keep = True
            else:
                keep = not size_is_too_small(size, args.min_size) if isinstance(size, str) else True

            if not keep:
                too_small += 1
                continue

            type_label = "main" if include_flag == "yes" else "single"
            size_str = size if isinstance(size, str) else ""
            targets_csv = ",".join(sorted(include_targets)) if isinstance(include_targets, set) else ""
            out_handle.write(f"{rec['raw_url']}\t{size_str}\t{type_label}\t{targets_csv}\n")
            kept += 1
            if include_flag == "yes":
                kept_main += 1
            else:
                kept_single += 1
    finally:
        if close_output:
            out_handle.close()

    if close_output and args.output:
        print(f"Filtered ledgers written to {args.output}", file=sys.stderr)

    print("=== filter summary ===", file=sys.stderr)
    print(f"Input rows:            {input_rows}", file=sys.stderr)
    print(f"Deduped (dropped):     {dedup_dropped}", file=sys.stderr)
    print(f"Include rows:          {main_rows}", file=sys.stderr)
    print(f"Include targets skipped (kept for later fetch): {include_targets_skipped}", file=sys.stderr)
    print(f"Too small (dropped):   {too_small}", file=sys.stderr)
    print(f"Total kept:            {kept}", file=sys.stderr)
    print(f"  kept main:           {kept_main}", file=sys.stderr)
    print(f"  kept single:         {kept_single}", file=sys.stderr)


def get_ledgers(args: argparse.Namespace) -> None:
    source_file = Path(args.sources)
    dest_dir = Path(args.dest)
    meta_path = Path(args.meta) if args.meta else dest_dir / ".ledger_meta"

    allowed_types = None
    if args.types:
        allowed_types = {t.strip().lower() for t in args.types.split(",") if t.strip()}

    urls, skipped_by_type = read_source_urls(source_file, allowed_types)
    if not urls:
        raise GitHubRequestError(0, f"No sources to process in {source_file}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    meta = load_meta(meta_path)

    # Count basenames for collision detection.
    basename_count: Dict[str, int] = {}
    for url in urls:
        base = url.rsplit("/", 1)[-1]
        basename_count[base] = basename_count.get(base, 0) + 1

    added = 0
    updated = 0
    skipped_not_modified = 0
    skipped_same = 0
    errors = 0

    for url in urls:
        base = url.rsplit("/", 1)[-1]
        dest_name = base
        if basename_count.get(base, 0) > 1:
            dest_name = slug_for_url(url, base)
            print(f"COLLISION: {base} -> {dest_name}")

        dest_path = dest_dir / dest_name

        old_etag, old_last = meta.get(url, ("", ""))

        print(f"FETCH: {url}")
        print(f"  -> {dest_name}")

        status, body, headers = fetch_url(url, old_etag, old_last)

        if status == 304:
            if dest_path.is_file():
                print(f"SKIP (not modified): {dest_name}")
                skipped_not_modified += 1
                continue
            # No local copy; re-fetch without conditional headers.
            status, body, headers = fetch_url(url, "", "")

        if status < 200 or status >= 300:
            print(f"ERROR: HTTP {status} for {url} (target {dest_name})", file=sys.stderr)
            errors += 1
            continue

        if not body:
            print(f"WARN: Empty download for {url}", file=sys.stderr)
            errors += 1
            continue

        body_with_comment = body + b"\n; Source: " + url.encode("utf-8") + b"\n"

        action = "ADD"
        if dest_path.is_file():
            try:
                existing = dest_path.read_bytes()
                if existing == body_with_comment:
                    print(f"SKIP (unchanged content): {dest_name}")
                    skipped_same += 1
                    continue
            except OSError:
                pass
            action = "UPDATE"
        elif old_etag or old_last:
            action = "UPDATE"

        try:
            dest_path.write_bytes(body_with_comment)
        except OSError as exc:
            print(f"ERROR: Failed to write {dest_path}: {exc}", file=sys.stderr)
            errors += 1
            continue

        new_etag = headers.get("etag", "")
        new_last = headers.get("last-modified", "")
        meta[url] = (new_etag, new_last)

        if action == "UPDATE":
            updated += 1
        else:
            added += 1
        print(f"{action}: {dest_name}")

    write_meta(meta_path, meta)

    print("=== get summary ===", file=sys.stderr)
    print(f"Sources processed:     {len(urls)}", file=sys.stderr)
    if allowed_types is not None:
        print(f"Skipped by type:       {skipped_by_type}", file=sys.stderr)
    print(f"Added:                 {added}", file=sys.stderr)
    print(f"Updated:               {updated}", file=sys.stderr)
    print(f"Skipped not modified:  {skipped_not_modified}", file=sys.stderr)
    print(f"Skipped unchanged:     {skipped_same}", file=sys.stderr)
    print(f"Errors:                {errors}", file=sys.stderr)
    print(f"Destination:           {dest_dir}", file=sys.stderr)


def rewrite_includes(main_path: Path, base_dir: str, target_map: Dict[str, str]) -> int:
    if not main_path.is_file():
        return 0
    try:
        lines = main_path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except OSError:
        return 0

    include_re = re.compile(r'^(\s*include\s+")([^"]+)(".*)$', re.IGNORECASE)
    rewritten = 0
    new_lines: List[str] = []
    for line in lines:
        if re.match(r"^\s*[;#]", line):
            new_lines.append(line)
            continue
        stripped = line.rstrip("\n")
        m = include_re.match(stripped)
        if not m:
            new_lines.append(line)
            continue
        include_target = m.group(2)
        resolved = resolve_include_path(base_dir, include_target)
        if resolved in target_map:
            new_target = target_map[resolved]
            rewritten += 1
            new_line = f"{m.group(1)}{new_target}{m.group(3)}"
            if line.endswith("\n"):
                new_line += "\n"
            new_lines.append(new_line)
        else:
            new_lines.append(line)
    try:
        main_path.write_text("".join(new_lines), encoding="utf-8")
    except OSError:
        return rewritten
    return rewritten


def includes(args: argparse.Namespace) -> None:
    source_file = Path(args.sources)
    dest_dir = Path(args.dest)

    allowed_types_for_counts = {t.strip().lower() for t in args.types.split(",") if t.strip()}
    include_types = {t.strip().lower() for t in args.include_types.split(",") if t.strip()}
    if not allowed_types_for_counts:
        allowed_types_for_counts = {"main", "single"}
    if not include_types:
        include_types = {"main"}

    records = parse_filtered_records(source_file)
    if not records:
        raise GitHubRequestError(0, f"No records found in {source_file}")

    # Basename counts across allowed types to match get() collision handling.
    basename_count: Dict[str, int] = {}
    for rec in records:
        rec_type = rec.get("type")
        if isinstance(rec_type, str) and rec_type in allowed_types_for_counts:
            base = rec.get("basename")
            if isinstance(base, str):
                basename_count[base] = basename_count.get(base, 0) + 1

    # Collect include tasks and rewrite mappings.
    include_tasks: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    main_states: Dict[str, Dict[str, object]] = {}
    include_targets_seen = 0

    for rec in records:
        rec_type = rec.get("type")
        if not (isinstance(rec_type, str) and rec_type in include_types):
            continue
        raw_url = rec.get("raw_url")
        owner = rec.get("owner")
        repo = rec.get("repo")
        sha = rec.get("sha")
        path_part = rec.get("path")
        base_dir = rec.get("base_dir")
        basename = rec.get("basename")
        include_targets = rec.get("include_targets", [])

        if not all(isinstance(x, str) for x in (raw_url, owner, repo, sha, path_part, basename)):
            continue

        base = basename  # type: ignore[assignment]
        dest_name = base
        if basename_count.get(base, 0) > 1:
            dest_name = slug_for_url(raw_url, base)  # type: ignore[arg-type]

        dest_path = dest_dir / dest_name
        subdir_name = Path(dest_name).stem
        subdir = dest_dir / subdir_name

        target_map: Dict[str, str] = {}
        include_dest_paths: List[Path] = []
        if isinstance(include_targets, list):
            for tgt in include_targets:
                if not isinstance(tgt, str) or not tgt:
                    continue
                include_targets_seen += 1
                safe_rel = sanitize_include_relpath(tgt.lstrip("/"))
                new_target = f"{subdir_name}/{safe_rel}"
                target_map[tgt] = new_target
                key = (owner, repo, sha, tgt)
                if key not in include_tasks:
                    include_tasks[key] = {
                        "url": f"https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{tgt}",
                        "dest_path": subdir / safe_rel,
                        "main_key": dest_name,
                    }
                    include_dest_paths.append(subdir / safe_rel)

        main_states[dest_name] = {
            "path": dest_path,
            "base_dir": base_dir,
            "target_map": target_map,
            "subdir": subdir,
            "include_paths": set(include_dest_paths),
            "failed": False,
        }

    if not include_tasks:
        print("No include targets to download.", file=sys.stderr)
        return

    downloads = 0
    download_errors = 0
    include_added = 0
    include_updated = 0
    include_skipped_same = 0
    for task_key, task in include_tasks.items():
        url = task["url"]
        dest_path = task["dest_path"]
        main_key = task.get("main_key")
        if not isinstance(url, str) or not isinstance(dest_path, Path):
            continue
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        main_state = main_states.get(main_key) if isinstance(main_key, str) else None
        status, body, headers = fetch_url(url, "", "")
        print(f"FETCH include: {url}")
        print(f"  -> {dest_path}")

        if status < 200 or status >= 300 or not body:
            print(f"ERROR: HTTP {status} for include {url}", file=sys.stderr)
            download_errors += 1
            if main_state is not None:
                main_state["failed"] = True
            continue
        body_with_comment = body + b"\n; Source: " + url.encode("utf-8") + b"\n"
        action = "ADD"
        if dest_path.is_file():
            try:
                existing = dest_path.read_bytes()
                if existing == body_with_comment:
                    print(f"SKIP (unchanged include): {dest_path}")
                    include_skipped_same += 1
                    continue
            except OSError:
                pass
            action = "UPDATE"

            try:
                dest_path.write_bytes(body_with_comment)
                downloads += 1
                if action == "UPDATE":
                    include_updated += 1
                else:
                    include_added += 1
                print(f"{action} include: {dest_path}")
                if main_state is not None:
                    include_paths = main_state.get("include_paths")
                    if isinstance(include_paths, set):
                        include_paths.add(dest_path)
            except OSError as exc:
                print(f"ERROR: Failed to write include {dest_path}: {exc}", file=sys.stderr)
                download_errors += 1
                if main_state is not None:
                    main_state["failed"] = True

    rewrites = 0
    missing_main = 0
    for dest_name, state in main_states.items():
        main_path = state.get("path")
        base_dir = state.get("base_dir")
        target_map = state.get("target_map")
        subdir = state.get("subdir")
        if not isinstance(main_path, Path) or not isinstance(base_dir, str) or not isinstance(target_map, dict):
            continue
        if not main_path.is_file():
            missing_main += 1
            continue
        # Ensure subdir exists even if downloads failed earlier.
        if isinstance(subdir, Path):
            subdir.mkdir(parents=True, exist_ok=True)
        rewrites += rewrite_includes(main_path, base_dir, target_map)

    broken_main_moved = 0
    broken_includes_moved = 0
    broken_dir = dest_dir / "broken"
    for dest_name, state in main_states.items():
        if not state.get("failed"):
            continue
        main_path = state.get("path")
        include_paths = state.get("include_paths", set())
        if not isinstance(main_path, Path):
            continue
        paths_to_move = [main_path]
        if isinstance(include_paths, set):
            paths_to_move.extend([p for p in include_paths if isinstance(p, Path)])
        for p in paths_to_move:
            try:
                rel = p.relative_to(dest_dir)
            except ValueError:
                continue
            target = broken_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                target.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            try:
                p.replace(target)
                if p == main_path:
                    broken_main_moved += 1
                else:
                    broken_includes_moved += 1
                print(f"MOVED to broken: {p} -> {target}")
            except OSError as exc:
                print(f"ERROR: Failed to move {p} to {target}: {exc}", file=sys.stderr)

    print("=== includes summary ===", file=sys.stderr)
    print(f"Include targets declared: {include_targets_seen}", file=sys.stderr)
    print(f"Include files downloaded: {downloads}", file=sys.stderr)
    print(f"  added:                 {include_added}", file=sys.stderr)
    print(f"  updated:               {include_updated}", file=sys.stderr)
    print(f"  skipped unchanged:     {include_skipped_same}", file=sys.stderr)
    print(f"Download errors:          {download_errors}", file=sys.stderr)
    print(f"Main files rewritten:     {rewrites}", file=sys.stderr)
    print(f"Main files missing:       {missing_main}", file=sys.stderr)
    print(f"Broken main files moved:  {broken_main_moved}", file=sys.stderr)
    print(f"Broken include files moved: {broken_includes_moved}", file=sys.stderr)


def sync_baselines(args: argparse.Namespace) -> None:
    ledgers_dir = Path(args.ledgers)
    baselines_dir = Path(args.output)
    sql_script = Path(args.sql_script)
    debug = args.debug
    venv_path = Path(args.venv) if args.venv else None
    setup_venv = args.setup_venv
    move_on_fail = args.move_broken

    baselines_dir.mkdir(parents=True, exist_ok=True)

    python_exe = Path(sys.executable)

    def venv_python(path: Path) -> Path:
        if os.name == "nt":
            candidate = path / "Scripts" / "python.exe"
        else:
            candidate = path / "bin" / "python"
        return candidate

    if venv_path:
        if not venv_path.exists() and setup_venv:
            try:
                subprocess.check_call([sys.executable, "-m", "venv", "--system-site-packages", str(venv_path)])
            except subprocess.CalledProcessError as exc:
                print(f"ERROR: Failed to create venv at {venv_path}: {exc}", file=sys.stderr)
                return
            try:
                subprocess.check_call(
                    [
                        str(venv_python(venv_path)),
                        "-m",
                        "pip",
                        "install",
                        "--no-build-isolation",
                        "--no-deps",
                        ".",
                    ],
                    cwd=sql_script.parent.parent,  # third_party/beancount
                )
            except subprocess.CalledProcessError as exc:
                print(f"ERROR: Failed to install beancount into venv: {exc}", file=sys.stderr)
                return
        python_exe = venv_python(venv_path)

    if not python_exe.exists():
        print(f"ERROR: Python interpreter not found: {python_exe}", file=sys.stderr)
        return

    processed = 0
    added = 0
    updated = 0
    skipped_same = 0
    failed = 0
    broken_main_moved = 0
    broken_include_dirs_moved = 0

    broken_dir = ledgers_dir / "broken"

    for ledger_path in sorted(ledgers_dir.iterdir()):
        if ledger_path.name == "broken" or ledger_path.is_dir():
            continue
        if ledger_path.suffix.lower() not in (".beancount", ".bean"):
            continue
        processed += 1
        name = ledger_path.stem
        target = baselines_dir / f"{name}.sqlite"

        fd, temp_path_str = tempfile.mkstemp(prefix=f"{name}.", suffix=".sqlite", dir=baselines_dir)
        os.close(fd)
        temp_path = Path(temp_path_str)
        try:
            run_sql_to_sqlite(python_exe, sql_script, ledger_path, temp_path, debug)
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: Failed to generate sqlite for {ledger_path.name}: {exc}", file=sys.stderr)
            failed += 1
            try:
                temp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            if move_on_fail:
                # Move ledger and its include folder to broken so we skip next time.
                try:
                    rel = ledger_path.relative_to(ledgers_dir)
                    broken_target = broken_dir / rel
                    broken_target.parent.mkdir(parents=True, exist_ok=True)
                    ledger_path.replace(broken_target)
                    broken_main_moved += 1
                    print(f"MOVED to broken: {ledger_path} -> {broken_target}")
                except Exception as move_exc:
                    print(f"ERROR: Failed to move {ledger_path} to broken: {move_exc}", file=sys.stderr)

                include_dir = ledgers_dir / name
                if include_dir.is_dir():
                    broken_include_dir = broken_dir / include_dir.relative_to(ledgers_dir)
                    try:
                        if broken_include_dir.exists():
                            shutil.rmtree(broken_include_dir)
                        shutil.move(str(include_dir), str(broken_include_dir))
                        broken_include_dirs_moved += 1
                        print(f"MOVED include dir to broken: {include_dir} -> {broken_include_dir}")
                    except Exception as move_exc:
                        print(f"ERROR: Failed to move include dir {include_dir}: {move_exc}", file=sys.stderr)

                # Remove stale target if it exists.
                try:
                    target.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
            continue

        if not temp_path.is_file():
            print(f"ERROR: Temp sqlite missing for {ledger_path.name}", file=sys.stderr)
            failed += 1
            continue

        action = "ADD"
        if target.is_file():
            try:
                if target.read_bytes() == temp_path.read_bytes():
                    print(f"SKIP (unchanged sqlite): {target.name}")
                    skipped_same += 1
                    temp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    continue
            except OSError:
                pass
            action = "UPDATE"

        try:
            shutil.copyfile(temp_path, target)
            temp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            if action == "UPDATE":
                updated += 1
            else:
                added += 1
            print(f"{action}: {target.name}")
        except OSError as exc:
            print(f"ERROR: Failed to finalize {target.name}: {exc}", file=sys.stderr)
            failed += 1
            try:
                temp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            continue

    print("=== sync summary ===", file=sys.stderr)
    print(f"Ledgers processed:      {processed}", file=sys.stderr)
    print(f"Added:                  {added}", file=sys.stderr)
    print(f"Updated:                {updated}", file=sys.stderr)
    print(f"Skipped unchanged:      {skipped_same}", file=sys.stderr)
    print(f"Failed:                 {failed}", file=sys.stderr)
    print(f"Broken main moved:      {broken_main_moved}", file=sys.stderr)
    print(f"Broken include dirs moved: {broken_include_dirs_moved}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified CLI for Beancount JDBC testing pipeline utilities.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    find_parser = subparsers.add_parser(
        "find",
        help="Find public Beancount ledgers on GitHub (replacement for scripts/find_ledgers.sh).",
    )
    find_parser.add_argument(
        "-q",
        "--query",
        default=DEFAULT_QUERY,
        help=f"GitHub search query (default: {DEFAULT_QUERY})",
    )
    find_parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Write URLs to a file instead of stdout",
    )
    find_parser.add_argument(
        "-s",
        "--include-size",
        action="store_true",
        help="Include file size (adds one contents API call per result)",
    )
    find_parser.add_argument(
        "-i",
        "--check-include",
        action="store_true",
        help="Flag include directives (adds contents API call and emits include targets)",
    )
    find_parser.add_argument(
        "-p",
        "--per-page",
        type=int,
        default=DEFAULT_PER_PAGE,
        help="Results per page (1-100, default: 100)",
    )
    find_parser.add_argument(
        "-m",
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Maximum pages to fetch (default: 10; API caps at 1000 results).",
    )
    find_parser.add_argument(
        "-t",
        "--token",
        default=None,
        help="GitHub token (overrides GITHUB_TOKEN/GH_TOKEN)",
    )
    find_parser.set_defaults(func=find_ledgers)

    filter_parser = subparsers.add_parser(
        "filter",
        help="Filter ledger metadata (replacement for scripts/filter_ledgers.sh).",
    )
    filter_parser.add_argument(
        "input",
        help="Input TSV from the find step (raw_url size include_flag include_targets_csv).",
    )
    filter_parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Write filtered TSV to a file instead of stdout.",
    )
    filter_parser.add_argument(
        "--min-size",
        type=int,
        default=10240,
        help="Minimum size (bytes) to keep non-include ledgers (default: 10240).",
    )
    filter_parser.set_defaults(func=filter_ledgers)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    default_dest = repo_root / "jdbc/src/test/resources/regression/ledgers"

    get_parser = subparsers.add_parser(
        "get",
        help="Download main/single ledgers (replacement for scripts/get_ledgers.sh).",
    )
    get_parser.add_argument(
        "-s",
        "--sources",
        default=str(script_dir / "public_ledger_sources.txt"),
        help="Path to ledger source list (default: scripts/public_ledger_sources.txt).",
    )
    get_parser.add_argument(
        "-d",
        "--dest",
        default=str(default_dest),
        help="Destination directory for ledgers (default: jdbc/src/test/resources/regression/ledgers).",
    )
    get_parser.add_argument(
        "-m",
        "--meta",
        default=None,
        help="Path to metadata cache file (default: <dest>/.ledger_meta).",
    )
    get_parser.add_argument(
        "--types",
        default="main,single",
        help="Comma-separated ledger types to download when sources include a type column (default: main,single).",
    )
    get_parser.set_defaults(func=get_ledgers)

    includes_parser = subparsers.add_parser(
        "includes",
        help="Download include ledgers and rewrite main files to point to per-ledger subfolders.",
    )
    includes_parser.add_argument(
        "-s",
        "--sources",
        default=str(script_dir / "public_ledger_sources_filtered.txt"),
        help="Path to filtered ledger list (TSV from filter step).",
    )
    includes_parser.add_argument(
        "-d",
        "--dest",
        default=str(default_dest),
        help="Destination directory where main ledgers reside (default: jdbc/src/test/resources/regression/ledgers).",
    )
    includes_parser.add_argument(
        "--types",
        default="main,single",
        help="Ledger types to consider for name collision counts (default: main,single).",
    )
    includes_parser.add_argument(
        "--include-types",
        default="main",
        help="Ledger types whose include targets should be downloaded (default: main).",
    )
    includes_parser.set_defaults(func=includes)

    sql_default = repo_root / "third_party/beancount/experiments/sql/sql.py"
    baselines_default = repo_root / "jdbc/src/test/resources/regression/bean_sql"

    sync_parser = subparsers.add_parser(
        "sync",
        help="Generate SQLite baselines from ledgers (replacement for make_sqlites.sh).",
    )
    sync_parser.add_argument(
        "-l",
        "--ledgers",
        default=str(default_dest),
        help="Directory containing ledgers (default: jdbc/src/test/resources/regression/ledgers).",
    )
    sync_parser.add_argument(
        "-o",
        "--output",
        default=str(baselines_default),
        help="Directory to write SQLite baselines (default: jdbc/src/test/resources/regression/bean_sql).",
    )
    sync_parser.add_argument(
        "-s",
        "--sql-script",
        default=str(sql_default),
        help="Path to bean-sql sql.py (default: third_party/beancount/experiments/sql/sql.py).",
    )
    sync_parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug logging from sql.py.",
    )
    sync_parser.add_argument(
        "--venv",
        default=None,
        help="Path to a virtualenv to run bean-sql. If provided with --setup-venv, will be created/used automatically.",
    )
    sync_parser.add_argument(
        "--setup-venv",
        action="store_true",
        help="Create the venv (if missing) and install vendored beancount into it (no-build-isolation, no-deps).",
    )
    sync_parser.add_argument(
        "--move-broken",
        action="store_true",
        help="Move failed ledgers (and include dirs) to ledgers/broken. Default: do not move while debugging.",
    )
    sync_parser.set_defaults(func=sync_baselines)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        parser.exit(1)
    try:
        args.func(args)
    except GitHubRequestError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
