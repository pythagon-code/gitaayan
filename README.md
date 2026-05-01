# Giitaayan + IMDb Mapping Project

## Team

- Teammate 1: `Aryan Gupta` (`aryang11`)
- Teammate 2: `Akshan Mohaney` (`mohaney2`)
- Teammate 3: `Abhay Pokhriyal` (`abhayp4`)
- Teammate 4: `Satwik` (`SatwikN27`)

## Project spec

Goal: scrape New Giitaayan song records and produce a CSV with metadata plus IMDb IDs.

Required output:

- Source: `https://new.giitaayan.com` song catalog (about 15,602 songs)
- One row per song
- Song metadata columns
- IMDb title ID for film (`tt...`) where matched
- IMDb person IDs (`nm...`) for lyricist/composer/singer/picturized-on where matched

## Current status

- `test_imdb.csv` is a **test run limited to 200 rows**.
- Full CSV for the full ~15.6k songs is **in progress**.

## Main pipeline

- Script: `giitaayan_imdb_pipeline.py`
- Test notebook (preview): `test_imdb_viewer.ipynb`
- Pipeline notebook: `giitaayan_imdb_pipeline.ipynb`

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run commands

Quick test (200 rows, no lyrics):

```powershell
python giitaayan_imdb_pipeline.py --max-rows 200 --no-lyrics --out test_imdb.csv
```

Full run (all songs, no lyrics):

```powershell
python giitaayan_imdb_pipeline.py --no-lyrics --out giitaayan_full_imdb.csv
```

Optional full run with lyrics (slow):

```powershell
python giitaayan_imdb_pipeline.py --out giitaayan_full_imdb_with_lyrics.csv
```

## Output columns (core)

- Film: `film_imdb_id` (`tt...`)
- People: `lyricist_imdb_ids`, `composer_imdb_ids`, `singer_imdb_ids`, `picturized_on_imdb_ids` (`nm...`, pipe-separated)

