import json
from pathlib import Path
import re
import time
import urllib.parse
import urllib.request

_SPARQL_URL = "https://query.wikidata.org/sparql"
_WD_API = "https://www.wikidata.org/w/api.php"


def _escape_sparql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _load_cache(path: Path) -> dict:
    if not path.is_file():
        return {"film": {}, "person": {}}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _sparql(query: str, user_agent: str) -> dict:
    url = _SPARQL_URL + "?format=json&query=" + urllib.parse.quote(query)
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read().decode("utf-8"))


def _wb_api(params: dict, user_agent: str) -> dict:
    url = _WD_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def _album_match_variants(album: str) -> list[str]:
    a = album.strip()
    if not a:
        return []
    out = [a]
    if len(a) >= 3 and a[-1].lower() == a[-2].lower() and a[-1].isalpha():
        out.append(a[:-1])
    seen: dict[str, None] = {}
    for x in out:
        if x not in seen:
            seen[x] = None
    return list(seen.keys())


def _normalize_year(year_value: str | None) -> int | None:
    if not year_value:
        return None
    m = re.search(r"(19|20)\d{2}", str(year_value))
    if not m:
        return None
    return int(m.group(0))


def resolve_film_imdb(
    album: str,
    year_value: str | None,
    *,
    cache_path: Path,
    user_agent: str,
    sleep_s: float,
) -> tuple[str | None, str | None]:
    album = (album or "").strip()
    if not album:
        return None, None
    y = _normalize_year(year_value)
    cache = _load_cache(cache_path)
    key = json.dumps({"a": album, "y": y}, sort_keys=True)
    if key in cache["film"]:
        hit = cache["film"][key]
        return hit.get("imdb"), hit.get("wikidata")
    if sleep_s > 0:
        time.sleep(sleep_s)
    variants = _album_match_variants(album)
    ors = " || ".join(
        'CONTAINS(LCASE(?l), LCASE("' + _escape_sparql_literal(v) + '"))'
        for v in variants
    )
    if y is not None:
        date_clause = "?film wdt:P577 ?pub .\n  FILTER(YEAR(?pub) = " + str(y) + ")"
    else:
        date_clause = "OPTIONAL { ?film wdt:P577 ?pub . }"
    query = f"""
SELECT ?imdb ?film ?l WHERE {{
  ?film wdt:P31/wdt:P279* wd:Q11424 .
  ?film wdt:P345 ?imdb .
  {date_clause}
  ?film rdfs:label ?l .
  FILTER({ors})
}}
LIMIT 24
"""
    try:
        data = _sparql(query, user_agent)
    except Exception:
        cache["film"][key] = {"imdb": None, "wikidata": None}
        _save_cache(cache_path, cache)
        return None, None
    rows = data.get("results", {}).get("bindings", [])
    imdb = None
    wdid = None
    if rows:
        al = album.strip().lower()
        by_film: dict[str, tuple[int, dict]] = {}
        for row in rows:
            film_uri = (row.get("film") or {}).get("value") or ""
            lab = (row.get("l") or {}).get("value") or ""
            ll = lab.lower()
            if ll == al:
                score = 100
            elif ll.startswith(al) or al.startswith(ll):
                score = 80
            elif al in ll:
                score = 60
            else:
                score = 40
            prev = by_film.get(film_uri)
            if prev is None or score > prev[0]:
                by_film[film_uri] = (score, row)
        ranked = sorted(by_film.values(), key=lambda x: x[0], reverse=True)
        best = ranked[0][1] if ranked else None
        if best:
            imdb = best.get("imdb", {}).get("value")
            film_uri = best.get("film", {}).get("value", "")
            if film_uri.startswith("http://www.wikidata.org/entity/"):
                wdid = film_uri.rsplit("/", 1)[-1]
    cache["film"][key] = {"imdb": imdb, "wikidata": wdid}
    _save_cache(cache_path, cache)
    return imdb, wdid


def resolve_person_imdb(
    name: str,
    *,
    cache_path: Path,
    user_agent: str,
    sleep_s: float,
) -> str | None:
    name = (name or "").strip()
    if not name or len(name) < 2:
        return None
    cache = _load_cache(cache_path)
    if name in cache["person"]:
        return cache["person"][name].get("imdb")
    if sleep_s > 0:
        time.sleep(sleep_s)
    try:
        search = _wb_api(
            {
                "action": "wbsearchentities",
                "format": "json",
                "language": "en",
                "type": "item",
                "limit": 6,
                "search": name,
            },
            user_agent,
        )
    except Exception:
        cache["person"][name] = {"imdb": None}
        _save_cache(cache_path, cache)
        return None
    imdb = None
    for hit in search.get("search", []):
        qid = hit.get("id")
        if not qid:
            continue
        try:
            ent = _wb_api(
                {
                    "action": "wbgetentities",
                    "format": "json",
                    "props": "claims",
                    "ids": qid,
                },
                user_agent,
            )
        except Exception:
            continue
        entity = (ent.get("entities") or {}).get(qid) or {}
        claims = entity.get("claims") or {}
        p345 = claims.get("P345") or []
        if not p345:
            continue
        val = (p345[0].get("mainsnak", {}).get("datavalue", {}) or {}).get("value")
        if isinstance(val, str) and val.startswith("nm"):
            imdb = val
            break
    cache["person"][name] = {"imdb": imdb}
    _save_cache(cache_path, cache)
    return imdb


def split_person_field(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[,;]", value)
    out: list[str] = []
    for p in parts:
        q = p.strip()
        if not q:
            continue
        for sub in re.split(r"\s+and\s+", q, flags=re.I):
            t = sub.strip()
            if t:
                out.append(t)
    return out
