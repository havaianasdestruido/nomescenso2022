#!/usr/bin/env python3
"""
IBGE Censo 2022 "Nomes no Brasil" scraper.

Scrapes the name/surname frequency ranking served by the official API behind
https://censo2022.ibge.gov.br/nomes :

    GET https://servicodados.ibge.gov.br/api/v3/nomes/{censo}/localidade/{localidade}/ranking/{tipo}?page={n}

where:
    censo      census edition (2022)
    localidade 0 = Brasil, or an IBGE locality code (UF/municipality)
    tipo       "nome" (first names) or "sobrenome" (surnames)
    page       1..totalPages (30 items per page, server-imposed)

Each response looks like:

    {"count": 128458, "page": 1, "totalPages": 4282, "nextPage": 2,
     "previousPage": 0, "showingFrom": 1, "showingTo": 30,
     "items": [{"nome": "maria", "percent": 6.0491,
                "frequencia": 12284478, "rank": 1}, ...]}

Output is JSONL, one record per line, in ranking order:

    {"rank": 1, "nome": "maria", "frequencia": 12284478, "percent": 6.0491}

Modes
-----
1. Live (default): fetches pages over HTTP with retries and concurrency.
   Optionally caches every raw page on disk so a run can be resumed later.
2. Offline: with --from-cache it reads previously cached raw page files
   (JSON or Markdown-wrapped JSON) from a directory and merges them with the
   exact same parsing path used by live mode. Handy in network-restricted
   environments.
3. SQLite: with --from-sqlite it reads a SQLite dump of the API (schema:
   table "frequencias" with columns id_local, nome, tipo_nome, ano,
   frequencia — see e.g. github.com/alexsantee/nomes_do_brasil_2022) and
   rebuilds the full ranking. This reproduces the API ordering exactly:
   frequency DESC, name DESC on ties (verified against the live API), with
   competition ranking (ties share a rank, the next rank skips) and
   percent = round(frequencia / population * 100, 4).

Only the Python standard library is required.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API_URL = (
    "https://servicodados.ibge.gov.br/api/v3/nomes/{censo}"
    "/localidade/{localidade}/ranking/{tipo}?page={page}"
)
USER_AGENT = "ibge-nomes-scraper/1.0 (research; contact: see repo)"
PAGE_SIZE = 30  # fixed server-side; kept for documentation/validation
POP_BR_2022 = 203_080_756  # Censo 2022 resident population of Brazil

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


# --------------------------------------------------------------------------
# Fetching / parsing
# --------------------------------------------------------------------------
def fetch_page_live(url: str, retries: int = 5, backoff: float = 1.5,
                    timeout: float = 30.0) -> bytes:
    """GET *url* and return raw bytes, retrying on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last_exc = exc
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
                raise  # not transient
            if attempt < retries:
                time.sleep(backoff ** attempt)
    raise RuntimeError(f"giving up on {url}: {last_exc}")


def extract_json(text: str) -> dict:
    """Parse a page body that is either pure JSON or JSON wrapped in a
    markdown code fence (as produced by some fetch proxies)."""
    stripped = text.strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    m = _FENCE_RE.search(stripped)
    if m:
        return json.loads(m.group(1))
    # Last resort: first balanced {...} block
    start = stripped.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(stripped)):
            if stripped[i] == "{":
                depth += 1
            elif stripped[i] == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(stripped[start:i + 1])
    raise ValueError("no JSON object found in page body")


def normalize_item(item: dict, fallback_rank: int) -> dict:
    """Normalize one ranking item to the canonical output schema."""
    return {
        "rank": int(item.get("rank", fallback_rank)),
        "nome": str(item["nome"]),
        "frequencia": int(item["frequencia"]),
        "percent": float(item.get("percent", 0.0)),
    }


# --------------------------------------------------------------------------
# Live crawl
# --------------------------------------------------------------------------
def crawl_live(args) -> list[dict]:
    meta_url = API_URL.format(censo=args.censo, localidade=args.localidade,
                              tipo=args.tipo, page=1)
    meta = extract_json(fetch_page_live(meta_url).decode("utf-8"))
    total_pages = int(meta["totalPages"])
    total_items = int(meta["count"])
    needed_pages = total_pages
    if args.max_names:
        needed_pages = min(total_pages, -(-args.max_names // PAGE_SIZE))
    if args.max_pages:
        needed_pages = min(needed_pages, args.max_pages)
    print(f"[live] count={total_items} totalPages={total_pages} "
          f"fetching {needed_pages} pages ({needed_pages * PAGE_SIZE} slots)",
          file=sys.stderr)

    if args.cache_dir:
        cache_dir = Path(args.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        _write_cache(cache_dir, 1, meta)

    def worker(page_no: int):
        url = API_URL.format(censo=args.censo, localidade=args.localidade,
                             tipo=args.tipo, page=page_no)
        data = extract_json(fetch_page_live(url).decode("utf-8"))
        if args.cache_dir:
            _write_cache(Path(args.cache_dir), page_no, data)
        return page_no, data

    pages: dict[int, dict] = {1: meta}
    pending = [p for p in range(2, needed_pages + 1)]
    done = 1
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, p): p for p in pending}
        for fut in cf.as_completed(futures):
            page_no, data = fut.result()
            pages[page_no] = data
            done += 1
            if done % 50 == 0 or done == len(pages):
                print(f"[live] {done}/{needed_pages} pages", file=sys.stderr)

    return pages_to_records(pages, args.max_names)


def _write_cache(cache_dir: Path, page_no: int, data: dict) -> None:
    (cache_dir / f"page-{page_no:05d}.json").write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


# --------------------------------------------------------------------------
# Offline crawl (from cached page files)
# --------------------------------------------------------------------------
def crawl_from_cache(args) -> list[dict]:
    cache_dir = Path(args.cache_dir)
    if not cache_dir.is_dir():
        raise SystemExit(f"cache dir not found: {cache_dir}")
    files = sorted(p for p in cache_dir.glob("page-*.json"))
    if not files:
        raise SystemExit(f"no page-*.json files in {cache_dir}")
    pages: dict[int, dict] = {}
    for fp in files:
        data = extract_json(fp.read_text(encoding="utf-8"))
        pages[int(data.get("page", fp.stem.split("-")[1]))] = data
    print(f"[cache] loaded {len(pages)} cached pages from {cache_dir}",
          file=sys.stderr)
    return pages_to_records(pages, args.max_names)


def crawl_from_sqlite(args) -> list[dict]:
    """Rebuild the full ranking from a SQLite dump of the IBGE API."""
    db = Path(args.from_sqlite)
    if not db.is_file():
        raise SystemExit(f"sqlite file not found: {db}")
    con = sqlite3.connect(db)
    cur = con.cursor()
    localidade = int(args.localidade) if str(args.localidade).isdigit() else args.localidade
    censo = int(args.censo) if str(args.censo).isdigit() else args.censo
    rows = cur.execute(
        "SELECT nome, frequencia FROM frequencias "
        "WHERE id_local = ? AND ano = ? AND tipo_nome = ? "
        "ORDER BY frequencia DESC, nome DESC",
        (localidade, censo, args.tipo),
    ).fetchall()
    con.close()
    if not rows:
        raise SystemExit("no rows matched (check --localidade/--censo/--tipo)")
    population = args.population or POP_BR_2022
    print(f"[sqlite] {len(rows)} rows from {db} (localidade={args.localidade}, "
          f"ano={args.censo}, tipo={args.tipo})", file=sys.stderr)

    records: list[dict] = []
    rank = prev_freq = 0
    for i, (nome, freq) in enumerate(rows):
        if freq != prev_freq:
            rank, prev_freq = i + 1, freq
        records.append({
            "rank": rank,
            "nome": nome,
            "frequencia": freq,
            "percent": round(freq / population * 100, 4),
        })
    if args.max_names:
        records = records[:args.max_names]
    return records


def pages_to_records(pages: dict[int, dict], max_names: int | None) -> list[dict]:
    records: list[dict] = []
    for page_no in sorted(pages):
        data = pages[page_no]
        base = (page_no - 1) * PAGE_SIZE
        for i, item in enumerate(data.get("items", [])):
            records.append(normalize_item(item, base + i + 1))
    records.sort(key=lambda r: r["rank"])
    if max_names:
        records = records[:max_names]
    return records


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tipo", choices=["nome", "sobrenome"], default="nome",
                    help="ranking type (default: nome)")
    ap.add_argument("--censo", default="2022", help="census edition (default: 2022)")
    ap.add_argument("--localidade", default="0",
                    help="0 = Brasil, or an IBGE locality code (default: 0)")
    ap.add_argument("--max-names", type=int, default=None,
                    help="cap the number of output records (e.g. 10000)")
    ap.add_argument("--max-pages", type=int, default=None,
                    help="cap the number of fetched pages")
    ap.add_argument("--workers", type=int, default=8,
                    help="concurrent HTTP workers for live mode (default: 8)")
    ap.add_argument("--cache-dir", default=None,
                    help="write/read raw API pages to/from this directory")
    ap.add_argument("--from-cache", action="store_true",
                    help="offline mode: parse --cache-dir files instead of HTTP")
    ap.add_argument("--from-sqlite", default=None, metavar="DB",
                    help="offline mode: rebuild the ranking from a SQLite dump "
                         "with a 'frequencias' table instead of HTTP")
    ap.add_argument("--population", type=int, default=None,
                    help=f"population used for percent in --from-sqlite mode "
                         f"(default: {POP_BR_2022}, Brasil Censo 2022)")
    ap.add_argument("-o", "--output", default=None,
                    help="output JSONL path (default: "
                         "nomes_censo2022_<tipo>[_topN].jsonl)")
    args = ap.parse_args(argv)

    if args.from_cache and not args.cache_dir:
        ap.error("--from-cache requires --cache-dir")
    if args.from_cache and args.from_sqlite:
        ap.error("--from-cache and --from-sqlite are mutually exclusive")

    if args.from_sqlite:
        records = crawl_from_sqlite(args)
    elif args.from_cache:
        records = crawl_from_cache(args)
    else:
        records = crawl_live(args)

    out = args.output
    if not out:
        suffix = f"_top{args.max_names}" if args.max_names else ""
        out = f"nomes_censo2022_{args.tipo}{suffix}.jsonl"
    with open(out, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"[done] wrote {len(records)} records -> {out}", file=sys.stderr)
    if records:
        print(f"[check] rank1={records[0]['nome']} "
              f"last_rank={records[-1]['rank']} "
              f"unique={len({r['nome'] for r in records})}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
