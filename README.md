# Giitaayan lyrics export

This folder contains a small Python tool that builds a **CSV** of Hindi song metadata from [new.giitaayan.com](https://new.giitaayan.com/)’s public API, attaches **lyrics text** from the open [giit](https://github.com/v9y/giit) repository (the same source the website loads), and optionally enriches rows with **film and person identifiers** that align with IMDb-style ids (`tt…`, `nm…`) by resolving through [Wikidata](https://www.wikidata.org/).

## Prerequisites

- **Python 3.10 or newer** (the scripts use `str | None` style type hints).
- Network access to `db.giitaayan.com`, `raw.githubusercontent.com`, and (if you use `--wikidata`) `query.wikidata.org` and `www.wikidata.org`.

## Installation

Clone or open this directory, then install dependencies:

```bash
cd gitaayan
pip install -r requirements.txt
```

Using a virtual environment is recommended:

```bash
python -m venv .venv
```

**Windows (PowerShell):**

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**macOS / Linux:**

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick start

Export **metadata plus lyrics** to the default file `giitaayan_songs.csv`:

```bash
python giitaayan_export.py
```

Export to a specific path:

```bash
python giitaayan_export.py --out ./output/songs.csv
```

Run a **short test** (stops after *N* rows):

```bash
python giitaayan_export.py --max-rows 100 --out ./sample.csv
```

## Full run with Wikidata / IMDb-style ids

Film and person columns are **empty unless** you pass `--wikidata`. That mode is much slower (many HTTP requests) but fills `film_imdb_id`, `film_wikidata_id`, and the `*_imdb_ids` person columns where resolution succeeds.

```bash
python giitaayan_export.py --out ./songs_enriched.csv --wikidata
```

Results are cached in a JSON file (default `wikidata_imdb_cache.json`) so **re-runs skip** already resolved film/person lookups. Point it elsewhere with `--cache`.

## Command-line reference

| Option | Default | Description |
|--------|---------|-------------|
| `--out` | `giitaayan_songs.csv` | Output CSV path. Parent directories are created if needed. |
| `--page-size` | `500` | Number of song rows requested per API page (PostgREST `Range` header). |
| `--max-rows` | `0` | If greater than zero, stop after writing that many rows (useful for tests). |
| `--no-lyrics` | off | Do not fetch `.isb.txt` files; `lyrics_text` stays empty. Faster. |
| `--lyrics-sleep` | `0.05` | Seconds to sleep before each lyrics HTTP request (rate limiting). |
| `--wikidata` | off | Resolve film and credited people via Wikidata; fills id columns. |
| `--wikidata-sleep` | `0.35` | Seconds to sleep before each Wikidata-related request (per lookup). |
| `--cache` | `wikidata_imdb_cache.json` | JSON cache path for Wikidata resolutions. |

Show built-in help:

```bash
python giitaayan_export.py -h
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `GIITAAYAN_SUPABASE_ANON` | Optional. JWT **anon** key for `https://db.giitaayan.com`. If unset, the exporter uses the same public anon key shipped in the Giitaayan web client (suitable for read-only public data). If the site rotates keys, set this to the current value from their frontend bundle. |

**Windows (PowerShell), current session only:**

```powershell
$env:GIITAAYAN_SUPABASE_ANON = "paste-jwt-here"
python giitaayan_export.py
```

**macOS / Linux:**

```bash
export GIITAAYAN_SUPABASE_ANON="paste-jwt-here"
python giitaayan_export.py
```

## Output CSV

The file is **UTF-8 with BOM** (`utf-8-sig`) so Excel on Windows tends to open Devanagari text correctly.

### Columns

| Column | Source / meaning |
|--------|------------------|
| `id`, `isb_number`, `song_title`, `song_code`, `album`, `year`, `lyricist`, `composer`, `singer`, `musicians`, `picturized_on`, `category`, `transcribed_by`, `created_at` | From PostgREST `songs` rows. |
| `lyrics_text` | Text extracted from the Giit `.isb.txt` file: if markers `#indian` and `#endindian` exist, the body between them; otherwise the full file body. Empty if `--no-lyrics` or if the raw file is missing (404). |
| `film_imdb_id` | Wikidata **P345** for the matched film (typically a `tt…` id). |
| `film_wikidata_id` | Matched item id (e.g. `Q10983032`). |
| `lyricist_imdb_ids`, `composer_imdb_ids`, `singer_imdb_ids`, `picturized_on_imdb_ids` | Pipe-separated `nm…` ids from Wikidata **P345** per split name (see `wikidata_imdb.split_person_field`). |

Lyrics may contain **newlines**; fields are quoted in CSV as required by the standard.

## Where the data comes from

1. **Song table:** `GET https://db.giitaayan.com/rest/v1/songs` with Supabase-style headers (`apikey`, `Authorization: Bearer …`) and pagination via `Range` / `Range-Unit`.
2. **Lyrics:** `GET https://raw.githubusercontent.com/v9y/giit/master/docs/{isb_number}.isb.txt` (same pattern as the live site).
3. **Identifiers:** Wikidata [SPARQL endpoint](https://query.wikidata.org/) for films (title variants + release year + label scoring) and [Wikidata API](https://www.wikidata.org/wiki/Wikidata:Data_access) search + `P345` for people.

Row counts in the API can differ from numbers shown in old URLs or UI copy; use `Prefer: count=exact` on the API if you need an exact total.

## Performance and etiquette

- A **full export with lyrics** issues roughly one GitHub request per song. Use `--lyrics-sleep` if you see throttling.
- **`--wikidata`** multiplies traffic: several requests per film and per credited name. Keep `--wikidata-sleep` conservative; reuse `--cache` for incremental work.
- Respect [Giitaayan](https://new.giitaayan.com/) terms of use, [GitHub](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service) guidance on automated access, and [Wikidata](https://wikidata.org/wiki/Wikidata:Data_access) user-agent / usage expectations.

## Troubleshooting

- **`401` / API key errors from `db.giitaayan.com`:** Set `GIITAAYAN_SUPABASE_ANON` to the current anon JWT from the site’s JavaScript bundle.
- **Empty `lyrics_text`:** The ISB file may not exist on the `giit` default branch, or GitHub returned an error; try opening the URL manually in a browser.
- **Empty or wrong `film_imdb_id` / `*_imdb_ids`:** Wikidata coverage and string matching are imperfect (duplicate titles, spelling, joint credits like “Kalyanji-Anandji”). Edit `wikidata_imdb_cache.json` or delete specific cache keys and re-run with `--wikidata`.
- **Unicode in the terminal:** The CSV is correct UTF-8; if the console mangles output, open the CSV in an editor or Excel instead of relying on `print`.

## Project layout

| File | Role |
|------|------|
| `giitaayan_export.py` | CLI: fetch songs, lyrics, write CSV, optional Wikidata enrichment. |
| `wikidata_imdb.py` | Film/person resolution and JSON cache. |
| `requirements.txt` | Python dependency pin (`requests`). |

## License and attribution

Song lyrics and transliterations in the Giit repository are contributed by many people; cite Giitaayan / Giit and respect their licensing and attribution when redistributing derived datasets.
