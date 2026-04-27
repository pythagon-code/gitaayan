import argparse
import csv
import os
import re
import time
import urllib.parse
from pathlib import Path

import requests
from tqdm import tqdm, trange

from wikidata_imdb import resolve_film_imdb, resolve_person_imdb, split_person_field

DEFAULT_ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndyanptZXJuY2FndHV5aHVicGFlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI0MjI2ODYsImV4cCI6MjA0Nzk5ODY4Nn0.YxLVtKQIcBH8RRSMdMDRT1_p_5_pZFyOQx37NuhHZ6U"
REST_URL = "https://db.giitaayan.com/rest/v1/songs"
LYRICS_TMPL = "https://raw.githubusercontent.com/v9y/giit/master/docs/{isb}.isb.txt"
UA = "giitaayan-export/1.0 (+https://new.giitaayan.com; research)"


def _fetch_songs_page(
    headers: dict,
    offset: int,
    limit: int,
) -> tuple[list[dict], str | None]:
    h = dict(headers)
    h["Range-Unit"] = "items"
    h["Range"] = f"{offset}-{offset + limit - 1}"
    h["Prefer"] = "count=exact"
    r = requests.get(
        REST_URL + "?select=*&order=id.asc",
        headers=h,
        timeout=120,
    )
    r.raise_for_status()
    return r.json(), r.headers.get("Content-Range")


def _extract_lyrics_body(raw: str) -> str:
    if "#indian" in raw and "#endindian" in raw:
        a = raw.index("#indian") + len("#indian")
        b = raw.index("#endindian")
        return raw[a:b].strip()
    return raw.strip()


def _fetch_lyrics(isb: str, session: requests.Session, sleep_s: float) -> str:
    if sleep_s > 0:
        time.sleep(sleep_s)
    url = LYRICS_TMPL.format(isb=urllib.parse.quote(isb, safe=""))
    resp = session.get(url, timeout=60)
    if resp.status_code != 200:
        return ""
    return _extract_lyrics_body(resp.text)


def _join_ids(ids: list[str | None]) -> str:
    clean = [x for x in ids if x]
    return "|".join(clean)


def _parse_content_total(content_range: str | None) -> int | None:
    if not content_range:
        return None
    m = re.match(r"^\d+-\d+/(\d+|\*)$", content_range.strip())
    if not m or m.group(1) == "*":
        return None
    return int(m.group(1))


def _main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=Path("giitaayan_songs.csv"))
    p.add_argument("--cache", type=Path, default=Path("wikidata_imdb_cache.json"))
    p.add_argument("--page-size", type=int, default=500)
    p.add_argument("--max-rows", type=int, default=0)
    p.add_argument("--no-lyrics", action="store_true")
    p.add_argument("--lyrics-sleep", type=float, default=0.05)
    p.add_argument("--wikidata", action="store_true")
    p.add_argument("--wikidata-sleep", type=float, default=0.35)
    args = p.parse_args()
    key = os.environ.get("GIITAAYAN_SUPABASE_ANON", DEFAULT_ANON)
    base_headers = {
        "Accept": "application/json",
        "User-Agent": UA,
        "apikey": key,
        "Authorization": "Bearer " + key,
    }
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    total_written = 0
    fieldnames = [
        "id",
        "isb_number",
        "song_title",
        "song_code",
        "album",
        "year",
        "lyricist",
        "composer",
        "singer",
        "musicians",
        "picturized_on",
        "category",
        "transcribed_by",
        "created_at",
        "lyrics_text",
        "film_imdb_id",
        "film_wikidata_id",
        "lyricist_imdb_ids",
        "composer_imdb_ids",
        "singer_imdb_ids",
        "picturized_on_imdb_ids",
    ]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows, cr = _fetch_songs_page(base_headers, 0, args.page_size)
    if not rows:
        return
    total_in_db = _parse_content_total(cr)
    if args.max_rows:
        target_rows = args.max_rows
    elif total_in_db is not None:
        target_rows = total_in_db
    else:
        target_rows = None
    with args.out.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()

        def _write_song_row(row: dict) -> None:
            nonlocal total_written
            isb = str(row.get("isb_number") or "")
            lyrics = ""
            if not args.no_lyrics and isb:
                lyrics = _fetch_lyrics(isb, session, args.lyrics_sleep)
            film_imdb = None
            film_wd = None
            lyricist_ids: list[str | None] = []
            composer_ids: list[str | None] = []
            singer_ids: list[str | None] = []
            cast_ids: list[str | None] = []
            if args.wikidata:
                film_imdb, film_wd = resolve_film_imdb(
                    str(row.get("album") or ""),
                    str(row.get("year") or "") or None,
                    cache_path=args.cache,
                    user_agent=UA,
                    sleep_s=args.wikidata_sleep,
                )
                for n in split_person_field(row.get("lyricist")):
                    lyricist_ids.append(
                        resolve_person_imdb(
                            n,
                            cache_path=args.cache,
                            user_agent=UA,
                            sleep_s=args.wikidata_sleep,
                        )
                    )
                for n in split_person_field(row.get("composer")):
                    composer_ids.append(
                        resolve_person_imdb(
                            n,
                            cache_path=args.cache,
                            user_agent=UA,
                            sleep_s=args.wikidata_sleep,
                        )
                    )
                for n in split_person_field(row.get("singer")):
                    singer_ids.append(
                        resolve_person_imdb(
                            n,
                            cache_path=args.cache,
                            user_agent=UA,
                            sleep_s=args.wikidata_sleep,
                        )
                    )
                for n in split_person_field(row.get("picturized_on")):
                    cast_ids.append(
                        resolve_person_imdb(
                            n,
                            cache_path=args.cache,
                            user_agent=UA,
                            sleep_s=args.wikidata_sleep,
                        )
                    )
            writer.writerow(
                {
                    "id": row.get("id"),
                    "isb_number": isb,
                    "song_title": row.get("song_title"),
                    "song_code": row.get("song_code"),
                    "album": row.get("album"),
                    "year": row.get("year"),
                    "lyricist": row.get("lyricist"),
                    "composer": row.get("composer"),
                    "singer": row.get("singer"),
                    "musicians": row.get("musicians"),
                    "picturized_on": row.get("picturized_on"),
                    "category": row.get("category"),
                    "transcribed_by": row.get("transcribed_by"),
                    "created_at": row.get("created_at"),
                    "lyrics_text": lyrics,
                    "film_imdb_id": film_imdb,
                    "film_wikidata_id": film_wd,
                    "lyricist_imdb_ids": _join_ids(lyricist_ids),
                    "composer_imdb_ids": _join_ids(composer_ids),
                    "singer_imdb_ids": _join_ids(singer_ids),
                    "picturized_on_imdb_ids": _join_ids(cast_ids),
                }
            )
            total_written += 1

        def _emit_page_batch(page_rows: list[dict]) -> bool:
            if args.max_rows:
                rem = args.max_rows - total_written
                if rem <= 0:
                    return False
                if rem < len(page_rows):
                    page_rows = page_rows[:rem]
            for row in tqdm(page_rows, desc="Songs", leave=False, unit="song"):
                _write_song_row(row)
            return True

        use_trange = target_rows is not None and args.page_size > 0
        if use_trange:
            num_pages = max(1, (target_rows + args.page_size - 1) // args.page_size)
            for page_idx in trange(num_pages, desc="Pages"):
                if page_idx > 0:
                    offset = page_idx * args.page_size
                    rows, _cr = _fetch_songs_page(
                        base_headers, offset, args.page_size
                    )
                    if not rows:
                        break
                if not _emit_page_batch(rows):
                    break
                if target_rows is not None and total_written >= target_rows:
                    break
                if (
                    total_in_db is not None
                    and (page_idx + 1) * args.page_size >= total_in_db
                ):
                    break
        else:
            offset = 0
            first = True
            with tqdm(unit="song", desc="Songs") as song_bar:
                while True:
                    if first:
                        first = False
                    else:
                        rows, _cr = _fetch_songs_page(
                            base_headers, offset, args.page_size
                        )
                    if not rows:
                        break
                    batch = rows
                    if args.max_rows:
                        rem = args.max_rows - total_written
                        if rem <= 0:
                            break
                        batch = rows[:rem]
                    for row in batch:
                        _write_song_row(row)
                        song_bar.update(1)
                    offset += len(rows)
                    if args.max_rows and total_written >= args.max_rows:
                        break
                    if len(rows) < args.page_size:
                        break


if __name__ == "__main__":
    _main()
