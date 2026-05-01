import argparse
import re
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

DEFAULT_ANON = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Indy"
    "anptZXJuY2FndHV5aHVicGFlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI0MjI2ODYsImV4cCI6"
    "MjA0Nzk5ODY4Nn0.YxLVtKQIcBH8RRSMdMDRT1_p_5_pZFyOQx37NuhHZ6U"
)
REST_URL = "https://db.giitaayan.com/rest/v1/songs"
LYRICS_TMPL = "https://raw.githubusercontent.com/v9y/giit/master/docs/{isb}.isb.txt"

TITLE_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
TITLE_AKAS_URL = "https://datasets.imdbws.com/title.akas.tsv.gz"
NAME_BASICS_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"


def split_person_field(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[,;]", value)
    out: list[str] = []
    for p in parts:
        q = p.strip()
        if not q:
            continue
        for sub in re.split(r"\s+and\s+|/|&", q, flags=re.I):
            t = sub.strip()
            if t:
                out.append(t)
    return out


def normalize_text(value: str | None) -> str:
    if value is None or pd.isna(value):
        return ""
    v = str(value).lower().strip()
    v = re.sub(r"[^a-z0-9]+", " ", v)
    return re.sub(r"\s+", " ", v).strip()


def extract_lyrics_body(raw: str) -> str:
    if "#indian" in raw and "#endindian" in raw:
        a = raw.index("#indian") + len("#indian")
        b = raw.index("#endindian")
        return raw[a:b].strip()
    return raw.strip()


def fetch_lyrics(isb: str, session: requests.Session) -> str:
    resp = session.get(LYRICS_TMPL.format(isb=isb), timeout=60)
    if resp.status_code != 200:
        return ""
    return extract_lyrics_body(resp.text)


def fetch_all_songs(headers: dict, page_size: int, max_rows: int) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        h = dict(headers)
        h["Range-Unit"] = "items"
        h["Range"] = f"{offset}-{offset + page_size - 1}"
        h["Prefer"] = "count=exact"
        r = requests.get(REST_URL + "?select=*&order=id.asc", headers=h, timeout=120)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        if max_rows > 0:
            rem = max_rows - len(rows)
            if rem <= 0:
                break
            batch = batch[:rem]
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < page_size:
            break
        if max_rows > 0 and len(rows) >= max_rows:
            break
    return rows


def build_film_mapping(songs_df: pd.DataFrame) -> dict[tuple[str, int | None], str]:
    film_keys_df = songs_df[["album", "year"]].copy()
    film_keys_df["album_norm"] = film_keys_df["album"].map(normalize_text)
    film_keys_df["year_int"] = pd.to_numeric(film_keys_df["year"], errors="coerce").astype("Int64")
    film_keys_df = film_keys_df[film_keys_df["album_norm"] != ""].drop_duplicates()
    album_set = set(film_keys_df["album_norm"].tolist())
    years = [int(y) for y in film_keys_df["year_int"].dropna().tolist()]
    min_year = min(years) - 3 if years else 1900
    max_year = max(years) + 3 if years else 2035

    # 1) title.basics.tsv.gz (load once, subset immediately)
    basics_hits: list[pd.DataFrame] = []
    chunks = pd.read_csv(
        TITLE_BASICS_URL,
        compression="gzip",
        sep="\t",
        dtype="string",
        na_values="\\N",
        usecols=["tconst", "titleType", "primaryTitle", "originalTitle", "startYear"],
        chunksize=300_000,
    )
    for chunk in tqdm(chunks, desc="IMDb title.basics"):
        chunk["primary_norm"] = chunk["primaryTitle"].map(normalize_text)
        chunk["original_norm"] = chunk["originalTitle"].map(normalize_text)
        year = pd.to_numeric(chunk["startYear"], errors="coerce")
        keep = (
            chunk["primary_norm"].isin(album_set)
            | chunk["original_norm"].isin(album_set)
        ) & year.between(min_year, max_year, inclusive="both")
        hit = chunk.loc[keep, ["tconst", "titleType", "startYear", "primary_norm", "original_norm"]]
        if not hit.empty:
            basics_hits.append(hit)
    basics_df = pd.concat(basics_hits, ignore_index=True) if basics_hits else pd.DataFrame(
        columns=["tconst", "titleType", "startYear", "primary_norm", "original_norm"]
    )
    basics_df["startYear"] = pd.to_numeric(basics_df["startYear"], errors="coerce").astype("Int64")

    # 2) title.akas.tsv.gz (load next, subset immediately)
    tconst_candidates = set(basics_df["tconst"].dropna().tolist())
    akas_hits: list[pd.DataFrame] = []
    chunks = pd.read_csv(
        TITLE_AKAS_URL,
        compression="gzip",
        sep="\t",
        dtype="string",
        na_values="\\N",
        usecols=["titleId", "title", "region", "language", "isOriginalTitle"],
        chunksize=300_000,
    )
    for chunk in tqdm(chunks, desc="IMDb title.akas"):
        chunk["title_norm"] = chunk["title"].map(normalize_text)
        in_region = chunk["region"].fillna("").str.upper().eq("IN")
        keep = chunk["title_norm"].isin(album_set) & in_region
        if tconst_candidates:
            # Retain known candidate tconst rows even if region is not IN.
            keep = keep | chunk["titleId"].isin(tconst_candidates)
        hit = chunk.loc[keep, ["titleId", "title_norm", "region", "language", "isOriginalTitle"]]
        if not hit.empty:
            akas_hits.append(hit)
    akas_df = pd.concat(akas_hits, ignore_index=True) if akas_hits else pd.DataFrame(
        columns=["titleId", "title_norm", "region", "language", "isOriginalTitle"]
    )

    film_map: dict[tuple[str, int | None], str] = {}
    basics_by_title = basics_df.melt(
        id_vars=["tconst", "titleType", "startYear"],
        value_vars=["primary_norm", "original_norm"],
        value_name="title_norm",
    )[["tconst", "titleType", "startYear", "title_norm"]]
    basics_by_title = basics_by_title[basics_by_title["title_norm"] != ""].drop_duplicates()

    candidates = basics_by_title.copy()
    if not akas_df.empty:
        akas_enriched = akas_df.merge(
            basics_df[["tconst", "titleType", "startYear"]],
            left_on="titleId",
            right_on="tconst",
            how="left",
        )
        akas_enriched = akas_enriched.drop(columns=["tconst"]).rename(
            columns={"titleId": "tconst"}
        )
        candidates = pd.concat(
            [candidates, akas_enriched[["tconst", "titleType", "startYear", "title_norm"]]],
            ignore_index=True,
        )
    candidates = candidates.dropna(subset=["tconst"]).drop_duplicates()

    grouped = candidates.groupby("title_norm", dropna=True)
    for row in film_keys_df.itertuples(index=False):
        title = row.album_norm
        year = int(row.year_int) if pd.notna(row.year_int) else None
        if title not in grouped.groups:
            film_map[(title, year)] = ""
            continue
        cand = grouped.get_group(title).copy()
        cand["score"] = 0
        cand["score"] += cand["titleType"].isin(["movie", "short"]).astype(int) * 3
        if year is not None:
            year_diff = (cand["startYear"] - year).abs()
            cand["score"] += (year_diff == 0).fillna(False).astype(int) * 4
            cand["score"] += (year_diff == 1).fillna(False).astype(int) * 2
            cand["score"] += (year_diff <= 2).fillna(False).astype(int)
        cand = cand.sort_values(["score"], ascending=False)
        film_map[(title, year)] = cand.iloc[0]["tconst"] if not cand.empty else ""
    return film_map


def build_person_mapping(songs_df: pd.DataFrame) -> dict[str, str]:
    people: set[str] = set()
    for col in ["lyricist", "composer", "singer", "picturized_on"]:
        for val in songs_df[col].fillna(""):
            for name in split_person_field(val):
                n = normalize_text(name)
                if n:
                    people.add(n)

    mapping: dict[str, str] = {}
    if not people:
        return mapping

    # 3) name.basics.tsv.gz (load after title files, subset immediately)
    chunks = pd.read_csv(
        NAME_BASICS_URL,
        compression="gzip",
        sep="\t",
        dtype="string",
        na_values="\\N",
        usecols=["nconst", "primaryName"],
        chunksize=300_000,
    )
    for chunk in tqdm(chunks, desc="IMDb name.basics"):
        chunk["name_norm"] = chunk["primaryName"].map(normalize_text)
        hit = chunk[chunk["name_norm"].isin(people)][["name_norm", "nconst"]]
        if hit.empty:
            continue
        for row in hit.itertuples(index=False):
            mapping.setdefault(row.name_norm, row.nconst)
    return mapping


def attach_person_ids(row: pd.Series, field: str, person_map: dict[str, str]) -> str:
    ids: list[str] = []
    for name in split_person_field(row.get(field)):
        nid = person_map.get(normalize_text(name), "")
        if nid:
            ids.append(nid)
    # keep order and deduplicate
    dedup = list(dict.fromkeys(ids))
    return "|".join(dedup)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("giitaayan_songs_imdb.csv"))
    ap.add_argument("--page-size", type=int, default=500)
    ap.add_argument("--max-rows", type=int, default=0)
    ap.add_argument("--no-lyrics", action="store_true")
    args = ap.parse_args()

    key = DEFAULT_ANON
    headers = {
        "Accept": "application/json",
        "apikey": key,
        "Authorization": "Bearer " + key,
        "User-Agent": "giitaayan-imdb-pipeline/1.0",
    }
    session = requests.Session()

    rows = fetch_all_songs(headers=headers, page_size=args.page_size, max_rows=args.max_rows)
    songs_df = pd.DataFrame(rows)
    if songs_df.empty:
        raise SystemExit("No songs returned from API.")

    if not args.no_lyrics:
        lyrics = []
        for isb in tqdm(songs_df["isb_number"].fillna("").astype(str), desc="Lyrics"):
            lyrics.append(fetch_lyrics(isb, session) if isb else "")
        songs_df["lyrics_text"] = lyrics
    else:
        songs_df["lyrics_text"] = ""

    film_map = build_film_mapping(songs_df)
    person_map = build_person_mapping(songs_df)

    songs_df["album_norm"] = songs_df["album"].map(normalize_text)
    songs_df["year_int"] = pd.to_numeric(songs_df["year"], errors="coerce").astype("Int64")
    songs_df["film_imdb_id"] = songs_df.apply(
        lambda r: film_map.get(
            (r["album_norm"], int(r["year_int"]) if pd.notna(r["year_int"]) else None),
            "",
        ),
        axis=1,
    )
    songs_df["lyricist_imdb_ids"] = songs_df.apply(
        lambda r: attach_person_ids(r, "lyricist", person_map), axis=1
    )
    songs_df["composer_imdb_ids"] = songs_df.apply(
        lambda r: attach_person_ids(r, "composer", person_map), axis=1
    )
    songs_df["singer_imdb_ids"] = songs_df.apply(
        lambda r: attach_person_ids(r, "singer", person_map), axis=1
    )
    songs_df["picturized_on_imdb_ids"] = songs_df.apply(
        lambda r: attach_person_ids(r, "picturized_on", person_map), axis=1
    )

    out_cols = [
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
        "lyricist_imdb_ids",
        "composer_imdb_ids",
        "singer_imdb_ids",
        "picturized_on_imdb_ids",
    ]
    for col in out_cols:
        if col not in songs_df.columns:
            songs_df[col] = ""
    final_df = songs_df[out_cols].copy()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(final_df)} rows to {args.out}")


if __name__ == "__main__":
    main()
