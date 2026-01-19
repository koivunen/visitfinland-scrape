#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
import psycopg


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def read_json(path: Optional[str], use_stdin: bool) -> Dict[str, Any]:
    if use_stdin:
        raw = sys.stdin.read()
        if not raw.strip():
            die("STDIN is empty.")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            die(f"Invalid JSON from STDIN: {e}")
    if not path:
        die("Provide --file PATH or use --stdin.")
        raise SystemExit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        die(f"File not found: {path}")
        raise SystemExit(1)
    except json.JSONDecodeError as e:
        die(f"Invalid JSON in file {path}: {e}")
        raise SystemExit(1)


def conninfo_from_env() -> str:
    load_dotenv()
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    sslmode = os.getenv("PGSSLMODE")

    missing = [k for k, v in [("PGHOST", host), ("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", password)] if not v]
    if missing:
        die(f"Missing required env vars: {', '.join(missing)}")

    parts = [f"host={host}", f"port={port}", f"dbname={db}", f"user={user}", f"password={password}"]
    if sslmode:
        parts.append(f"sslmode={sslmode}")
    return " ".join(parts)


def pick_name(product: Dict[str, Any], prefer_langs: Tuple[str, ...] = ("fi", "en")) -> Tuple[str, Optional[str]]:
    infos = product.get("productInformations") or []
    if not isinstance(infos, list):
        return "", None

    def norm_lang(x: Any) -> str:
        return (x or "").strip().lower()

    for lang in prefer_langs:
        for pi in infos:
            if not isinstance(pi, dict):
                continue
            name = (pi.get("name") or "").strip()
            if name and norm_lang(pi.get("language")) == lang:
                return name, pi.get("language")

    for pi in infos:
        if not isinstance(pi, dict):
            continue
        name = (pi.get("name") or "").strip()
        if name:
            return name, pi.get("language")

    return "", None


def parse_location_point(loc: Any) -> Optional[Tuple[float, float]]:
    # Gotcha: format is "(latitude,longitude)" (note order). PostGIS point needs (lon, lat).
    if loc is None:
        return None
    if not isinstance(loc, str):
        return None
    s = loc.strip()
    if not (s.startswith("(") and s.endswith(")")):
        return None
    inner = s[1:-1].strip()
    parts = [p.strip() for p in inner.split(",")]
    if len(parts) != 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except ValueError:
        return None
    return lat, lon


def extract_primary_address(product: Dict[str, Any]) -> Dict[str, Any]:
    addrs = product.get("postalAddresses") or []
    if isinstance(addrs, list) and addrs and isinstance(addrs[0], dict):
        return addrs[0]
    return {}


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.products (
              product_id               text PRIMARY KEY,
              product_name             text NOT NULL,
              product_name_language    text,
              company_business_name    text,
              product_type             text,
              webshop_url_primary      text,
              url_primary              text,
              accessible               boolean,
              updated_at               timestamptz,
              postal_code              text,
              street_name              text,
              city                     text,
              location                 geography(Point, 4326),
              raw                      jsonb NOT NULL,
              ingested_at              timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS products_location_gix ON public.products USING gist (location);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS products_updated_at_idx ON public.products (updated_at DESC);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS products_type_idx ON public.products (product_type);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS products_name_idx ON public.products (product_name);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS products_raw_gin ON public.products USING gin (raw jsonb_path_ops);"
        )


UPSERT_SQL = """
INSERT INTO public.products (
  product_id,
  product_name,
  product_name_language,
  company_business_name,
  product_type,
  webshop_url_primary,
  url_primary,
  accessible,
  updated_at,
  postal_code,
  street_name,
  city,
  location,
  raw
)
VALUES (
  %(product_id)s,
  %(product_name)s,
  %(product_name_language)s,
  %(company_business_name)s,
  %(product_type)s,
  %(webshop_url_primary)s,
  %(url_primary)s,
  %(accessible)s,
  %(updated_at)s,
  %(postal_code)s,
  %(street_name)s,
  %(city)s,
 CASE
  WHEN (%(lat)s::double precision) IS NULL OR (%(lon)s::double precision) IS NULL THEN NULL
  ELSE ST_SetSRID(
         ST_MakePoint(%(lon)s::double precision, %(lat)s::double precision),
         4326
       )::geography
END,
  %(raw)s::jsonb
)
ON CONFLICT (product_id) DO UPDATE SET
  product_name           = EXCLUDED.product_name,
  product_name_language  = EXCLUDED.product_name_language,
  company_business_name  = EXCLUDED.company_business_name,
  product_type           = EXCLUDED.product_type,
  webshop_url_primary    = EXCLUDED.webshop_url_primary,
  url_primary            = EXCLUDED.url_primary,
  accessible             = EXCLUDED.accessible,
  updated_at             = EXCLUDED.updated_at,
  postal_code            = EXCLUDED.postal_code,
  street_name            = EXCLUDED.street_name,
  city                   = EXCLUDED.city,
  location               = EXCLUDED.location,
  raw                    = EXCLUDED.raw,
  ingested_at            = now();
"""


def build_row(product: Dict[str, Any]) -> Dict[str, Any]:
    pid = (product.get("id") or "").strip()
    if not pid:
        die("Encountered product without id.")

    name, name_lang = pick_name(product)
    if not name:
        die(f"Product {pid} has no productInformations.name")

    company = None
    comp = product.get("company")
    if isinstance(comp, dict):
        company = comp.get("businessName")

    addr0 = extract_primary_address(product)
    loc_str = addr0.get("location") if isinstance(addr0, dict) else None
    latlon = parse_location_point(loc_str)

    lat = lon = None
    if latlon is not None:
        lat, lon = latlon

    return {
        "product_id": pid,
        "product_name": name,
        "product_name_language": name_lang,
        "company_business_name": company,
        "product_type": product.get("type"),
        "webshop_url_primary": product.get("webshopUrlPrimary"),
        "url_primary": product.get("urlPrimary"),
        "accessible": product.get("accessible"),
        "updated_at": product.get("updatedAt"),
        "postal_code": addr0.get("postalCode") if isinstance(addr0, dict) else None,
        "street_name": addr0.get("streetName") if isinstance(addr0, dict) else None,
        "city": addr0.get("city") if isinstance(addr0, dict) else None,
        "lat": lat,
        "lon": lon,
        "raw": json.dumps(product, ensure_ascii=False),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Insert GraphQL product JSON directly into PostgreSQL.")
    ap.add_argument("--file", help="Path to GraphQL JSON response file")
    ap.add_argument("--stdin", action="store_true", help="Read JSON from STDIN")
    ap.add_argument("--ensure-schema", action="store_true", help="Create table/indexes/extensions if missing")
    ap.add_argument("--commit-every", type=int, default=500, help="Commit every N rows (0 commits only at end)")
    args = ap.parse_args()

    payload = read_json(args.file, args.stdin)
    products = payload
    if not isinstance(products, list):
        die('JSON must contain array field "data.product".')
    if not products:
        die('No products found in "data.product".')
        raise SystemExit(1)
    
    conninfo = conninfo_from_env()

    with psycopg.connect(conninfo) as conn:
        if args.ensure_schema:
            ensure_schema(conn)
            conn.commit()

        n = 0
        with conn.cursor() as cur:
            for p in products:
                if not isinstance(p, dict):
                    continue
                row = build_row(p)
                cur.execute(UPSERT_SQL, row)
                n += 1
                if args.commit_every > 0 and (n % args.commit_every == 0):
                    conn.commit()

        conn.commit()
        print(f"OK: upserted {n} products.")


if __name__ == "__main__":
    main()
