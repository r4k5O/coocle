from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from . import db as dbmod
from .crawler import CrawlConfig, crawl_loop


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Coocle crawler: fetch pages into SQLite FTS index.")
    p.add_argument(
        "--db",
        default=os.environ.get("COOCLE_DB", ""),
        help="Path to SQLite DB (default: COOCLE_DB or ./data/coocle.db).",
    )
    p.add_argument(
        "--seeds",
        default="",
        help="Comma-separated seed URLs, e.g. https://example.com,https://example.org",
    )
    p.add_argument(
        "--seeds-file",
        default="",
        help="Path to text file with one seed URL per line (comments with # supported).",
    )
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--max-depth", type=int, default=2)
    p.add_argument("--delay", type=float, default=0.6, help="Delay between requests in seconds.")
    p.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("COOCLE_CRAWL_CONCURRENCY", "4")),
        help="Number of pages to crawl concurrently (default: 4).",
    )
    p.add_argument(
        "--embeddings",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Compute and store embeddings for vector search (default: false).",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO). Use DEBUG for details.",
    )
    p.add_argument(
        "--same-host-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restrict crawling to the same host(s) as the seeds (default: true).",
    )
    return p.parse_args()


def _load_seeds(args: argparse.Namespace) -> list[str]:
    seeds: list[str] = []
    if args.seeds:
        seeds.extend([s.strip() for s in str(args.seeds).split(",") if s.strip()])
    if args.seeds_file:
        path = Path(str(args.seeds_file))
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            seeds.append(line)
    deduped = list(dict.fromkeys(seeds))
    if not deduped:
        raise SystemExit("No seeds provided. Use --seeds or --seeds-file.")
    return deduped


async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db) if args.db else (root / "data" / "coocle.db")

    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)

    stop_event = asyncio.Event()
    seeds = _load_seeds(args)
    logging.getLogger("coocle.crawl").info("Loaded %d seed(s). DB=%s", len(seeds), db_path)
    cfg = CrawlConfig(
        max_pages=int(args.max_pages),
        max_depth=int(args.max_depth),
        delay_s=float(args.delay),
        same_host_only=bool(args.same_host_only),
        enable_embeddings=bool(args.embeddings),
        max_concurrency=max(1, int(args.concurrency)),
    )

    await crawl_loop(
        conn=conn,
        db=dbmod,
        seeds=seeds,
        cfg=cfg,
        stop_event=stop_event,
        run_forever=False,
    )
    conn.close()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()

