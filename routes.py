import json
import time
import asyncio
import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

import state
import storage
import providers
import upstream
import catalog_worker
from config import (
    API_VERSION, DEFAULT_HOST, KNOWN_HOSTS, API_BASE, DEFAULT_HEADERS,
    PLAYER_HEADERS, CATALOG_SECTIONS, TAB_PAYLOAD_VARIANTS,
    TAB_MOVIES, TAB_TV, TAB_ANIMATION,
    CATALOG_DB, CATALOG_REFRESH_INTERVAL, CATALOG_KEYWORDS,
    CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_SECONDS,
)
from helpers import (
    normalize_banner_item, normalize_subject_item, srt_to_vtt, pick_mp4_stream,
)

router = APIRouter()


# ─── PROVIDERS ────────────────────────────────────────────────────────────────

@router.get("/providers")
async def list_providers():
    now = time.time()
    entries = []
    for host, st in providers.provider_state.items():
        entries.append({
            "host": host,
            "alive": st["alive"],
            "circuit_open": st["circuit_open_until"] > now,
            "circuit_open_for": max(0, int(st["circuit_open_until"] - now)),
            "consecutive_failures": st["consecutive_failures"],
            "total_success": st["total_success"],
            "total_failure": st["total_failure"],
            "avg_latency_ms": int(st["avg_latency_ms"]),
            "last_success_ago": max(0, int(now - st["last_success"])) if st["last_success"] else None,
            "last_failure_ago": max(0, int(now - st["last_failure"])) if st["last_failure"] else None,
            "last_error": st["last_error"],
        })
    entries.sort(key=lambda p: KNOWN_HOSTS.index(p["host"]) if p["host"] in KNOWN_HOSTS else 999)
    return {
        "default_host": DEFAULT_HOST,
        "provider_count": len(entries),
        "healthy": sum(1 for p in entries if p["alive"] and not p["circuit_open"]),
        "degraded": sum(1 for p in entries if p["circuit_open"]),
        "providers": entries,
        "circuit_breaker": {
            "threshold": CIRCUIT_BREAKER_THRESHOLD,
            "window_seconds": CIRCUIT_BREAKER_SECONDS,
        },
    }


@router.post("/providers/probe")
async def probe_providers():
    results = await asyncio.gather(*(providers.probe_provider(h) for h in KNOWN_HOSTS))
    return {"probed": len(results), "results": results}


@router.post("/providers/reset")
async def reset_providers():
    for st in providers.provider_state.values():
        st["alive"] = True
        st["consecutive_failures"] = 0
        st["circuit_open_until"] = 0
        st["last_error"] = None
    return {"status": "reset", "providers": len(providers.provider_state)}


@router.get("/api/hosts")
async def legacy_hosts(refresh: bool = False):
    data = await list_providers()
    return {
        "primary": data["default_host"],
        "known_hosts": KNOWN_HOSTS,
        "results": [
            {"host": p["host"], "alive": p["alive"], "latency_ms": p["avg_latency_ms"]}
            for p in data["providers"]
        ],
        "unique_catalogs": 1,
    }


# ─── HOME ─────────────────────────────────────────────────────────────────────

@router.get("/home")
async def get_home(host: str = DEFAULT_HOST):
    ops = await catalog_worker.get_home_operating_list(host)
    sections = []
    for op in ops:
        op_type = op.get("type")
        title = op.get("title", "Featured")
        if op_type == "BANNER":
            items = [
                n for n in (
                    normalize_banner_item(i)
                    for i in op.get("banner", {}).get("items", []) or []
                ) if n
            ]
            sections.append({"section": "Banner", "count": len(items), "items": items})
        elif op_type in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            items = [
                n for n in (
                    normalize_subject_item(s)
                    for s in op.get("subjects", []) or []
                ) if n
            ]
            sections.append({"section": title, "count": len(items), "items": items})
    return {"status": "success", "host": host, "sections": sections}


@router.get("/home/all")
async def get_home_aggregate(dedupe: bool = True, host: str = DEFAULT_HOST):
    ops = await catalog_worker.get_home_operating_list(host)
    items: list[dict] = []
    seen: set[str] = set()
    for op in ops:
        op_type = op.get("type")
        if op_type == "BANNER":
            for raw in op.get("banner", {}).get("items", []) or []:
                norm = normalize_banner_item(raw)
                if not norm or not norm.get("subject_id"):
                    continue
                if dedupe and norm["subject_id"] in seen:
                    continue
                seen.add(norm["subject_id"])
                items.append(norm)
        elif op_type in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            for raw in op.get("subjects", []) or []:
                norm = normalize_subject_item(raw)
                if not norm or not norm.get("subject_id"):
                    continue
                if dedupe and norm["subject_id"] in seen:
                    continue
                seen.add(norm["subject_id"])
                items.append(norm)
    return {"count": len(items), "host": host, "items": items}


@router.get("/sections")
async def list_sections(host: str = DEFAULT_HOST):
    ops = await catalog_worker.get_home_operating_list(host)
    out = []
    for op in ops:
        op_type = op.get("type")
        title = op.get("title", "Featured")
        if op_type == "BANNER":
            count = len(op.get("banner", {}).get("items", []) or [])
        elif op_type in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            count = len(op.get("subjects", []) or [])
        else:
            count = 0
        out.append({"section": title, "type": op_type, "count": count})
    return {"host": host, "sections": out}


@router.get("/browse/{section}")
async def browse_section(section: str, page: int = 1, per_page: int = 24, host: str = DEFAULT_HOST):
    ops = await catalog_worker.get_home_operating_list(host)
    matched: list[dict] = []
    matched_titles: set[str] = set()
    for op in ops:
        op_type = op.get("type")
        title = op.get("title", "")
        if section.lower() not in title.lower():
            continue
        if op_type not in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            continue
        matched_titles.add(title)
        for raw in op.get("subjects", []) or []:
            item = normalize_subject_item(raw)
            if item and item.get("subject_id"):
                matched.append(item)
    if not matched:
        raise HTTPException(404, detail=f"No section matching: {section!r}. Try /sections.")
    start = (page - 1) * per_page
    paged = matched[start:start + per_page]
    return {
        "section": section,
        "host": host,
        "matched_titles": sorted(matched_titles),
        "page": page,
        "per_page": per_page,
        "total": len(matched),
        "items": paged,
    }


# ─── CATALOG ──────────────────────────────────────────────────────────────────

def _row_to_item(r) -> dict:
    return {
        "name": r["title"],
        "poster_url": r["poster_url"],
        "slug": r["slug"],
        "subject_id": r["subject_id"],
        "rating": r["rating"],
        "year": r["year"],
        "badge": r["badge"],
        "subject_type": r["subject_type"] if "subject_type" in r.keys() else None,
        "genre": r["genre"] if "genre" in r.keys() else None,
        "source_host": r["source_host"] if "source_host" in r.keys() else None,
        "types": [
            t for t, f in (("movie", r["is_movie"]), ("tv", r["is_tv"]), ("anime", r["is_anime"])) if f
        ],
    }


@router.get("/catalog")
async def catalog(
    type: str = "all",
    sort: str = "recent",
    page: int = 1,
    per_page: int = 24,
    q: str | None = None,
    host: str | None = None,
):
    where = []
    params: list = []
    if type == "movie":
        where.append("is_movie = 1")
    elif type == "tv":
        where.append("is_tv = 1")
    elif type == "anime":
        where.append("is_anime = 1")
    elif type not in ("all", ""):
        raise HTTPException(400, detail=f"Unknown type: {type} (all|movie|tv|anime)")
    if q:
        fts_ids = storage.fts_search(q)
        if fts_ids is None:
            where.append("title LIKE ?")
            params.append(f"%{q}%")
        elif fts_ids:
            placeholders = ",".join("?" * len(fts_ids))
            where.append(f"subject_id IN ({placeholders})")
            params.extend(fts_ids)
        else:
            where.append("1 = 0")
    if host:
        where.append("source_host = ?")
        params.append(host)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if sort == "title":
        order = "title COLLATE NOCASE ASC"
    elif sort == "rating":
        order = "CAST(rating AS REAL) DESC NULLS LAST"
    elif sort == "recent":
        order = "last_seen DESC"
    else:
        raise HTTPException(400, detail=f"Unknown sort: {sort} (recent|title|rating)")

    per_page = max(1, min(per_page, 100))
    offset = (max(1, page) - 1) * per_page
    total = storage.read_scalar(f"SELECT COUNT(*) FROM subjects {where_sql}", tuple(params)) or 0
    rows = storage.read(
        f"""
        SELECT subject_id, title, poster_url, slug, rating, year, badge,
               is_movie, is_tv, is_anime, subject_type, genre, source_host
        FROM subjects {where_sql}
        ORDER BY {order}
        LIMIT ? OFFSET ?
        """,
        tuple(params + [per_page, offset]),
    )
    return {
        "type": type,
        "sort": sort,
        "query": q,
        "host": host,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": (total + per_page - 1) // per_page if per_page else 0,
        "items": [_row_to_item(r) for r in rows],
    }


@router.get("/catalog/stats")
async def catalog_stats():
    total = storage.read_scalar("SELECT COUNT(*) FROM subjects") or 0
    movies = storage.read_scalar("SELECT COUNT(*) FROM subjects WHERE is_movie = 1") or 0
    tv = storage.read_scalar("SELECT COUNT(*) FROM subjects WHERE is_tv = 1") or 0
    anime = storage.read_scalar("SELECT COUNT(*) FROM subjects WHERE is_anime = 1") or 0
    untyped = storage.read_scalar(
        "SELECT COUNT(*) FROM subjects WHERE is_movie = 0 AND is_tv = 0 AND is_anime = 0"
    ) or 0
    untyped_pending = storage.read_scalar(
        "SELECT COUNT(*) FROM subjects WHERE subject_type IS NULL AND slug IS NOT NULL"
    ) or 0
    subject_types = {
        str(r[0]): r[1] for r in storage.read(
            "SELECT subject_type, COUNT(*) FROM subjects WHERE subject_type IS NOT NULL GROUP BY subject_type"
        )
    }
    by_host = {
        r[0]: r[1] for r in storage.read(
            "SELECT source_host, COUNT(*) FROM subjects GROUP BY source_host"
        )
    }
    kw_rows = storage.read(
        "SELECT keyword, pages_walked, items_seen, hit_cap, total_per_upstream FROM keyword_stats ORDER BY keyword"
    )
    keyword_stats = {
        r["keyword"]: {
            "pages": r["pages_walked"],
            "items": r["items_seen"],
            "hit_cap": bool(r["hit_cap"]),
            "upstream_total": r["total_per_upstream"],
        }
        for r in kw_rows
    }
    return {
        "total": total,
        "movies": movies,
        "tv": tv,
        "anime": anime,
        "untyped": untyped,
        "untyped_pending_enrichment": untyped_pending,
        "subject_types": subject_types,
        "by_host": by_host,
        "db_path": CATALOG_DB,
        "refresh_interval_seconds": CATALOG_REFRESH_INTERVAL,
        "keywords_total": len(CATALOG_KEYWORDS),
        "known_hosts": KNOWN_HOSTS,
        "walk_state": state.walk_state,
        "keyword_stats": keyword_stats,
    }


@router.post("/catalog/refresh")
async def catalog_refresh(mode: str = "full", force: bool = False):
    if mode == "enrich":
        if state.walk_state.get("enriching"):
            return {"status": "enrich_already_running", "walk_state": state.walk_state}
        asyncio.create_task(catalog_worker.run_enrichment())
        return {"status": "enrich_queued"}
    if state.walk_state.get("running"):
        return {"status": "walk_already_running", "walk_state": state.walk_state}

    async def _full():
        await catalog_worker.run_full_walk(force=force)
        await catalog_worker.run_enrichment()

    asyncio.create_task(_full())
    return {"status": "full_queued", "force": force}


@router.post("/catalog/add")
async def catalog_add(slug: str, host: str | None = None):
    h = host or DEFAULT_HOST
    data = await upstream.bff_request(f"/detail?detailPath={slug}", preferred_host=h)
    sub = (data.get("data") or {}).get("subject") or {}
    if not sub.get("subjectId"):
        raise HTTPException(502, detail="Detail response has no subject")
    storage.upsert_subject(sub, source_host=h)
    if state.write_conn is not None:
        state.write_conn.commit()
    row = storage.read(
        "SELECT subject_id, slug, source_host FROM subjects WHERE subject_id=?",
        (str(sub["subjectId"]),),
    )
    if row:
        await catalog_worker.enrich_subject(row[0])
        if state.write_conn is not None:
            state.write_conn.commit()
    return {
        "status": "added",
        "subject_id": str(sub["subjectId"]),
        "title": sub.get("title"),
        "subject_type": sub.get("subjectType"),
        "genre": sub.get("genre"),
        "host": h,
    }


# ─── MOVIES / TV / ANIMATION (home fallback) ──────────────────────────────────

async def _get_category_data(tab_id: int, page: int = 1, per_page: int = 24, sort: str = "RECOMMEND") -> dict:
    payload = {
        "tabId": tab_id,
        "filter": {
            "sort": sort,
            "genre": "ALL",
            "country": "ALL",
            "year": "ALL",
            "language": "ALL",
        },
        "page": page,
        "perPage": per_page,
    }
    data = await upstream.bff_request("/subject/filter", method="POST", payload=payload)
    inner = data.get("data", {}) or {}
    raw_items = inner.get("items") or inner.get("subjects") or []
    items = [
        {
            "name": sub.get("title"),
            "poster_url": (sub.get("cover") or {}).get("url"),
            "slug": sub.get("detailPath"),
            "subject_id": sub.get("subjectId"),
            "badge": sub.get("corner"),
            "rating": sub.get("imdbRatingValue"),
            "year": (sub.get("releaseDate") or "")[:4] or None,
        }
        for sub in raw_items
    ]
    pager = inner.get("pager", {}) or {}
    total = pager.get("totalCount") or inner.get("total") or len(items)
    sentinel = bool(total and total >= 1000000)
    return {
        "tab_id": tab_id,
        "page": page,
        "per_page": per_page,
        "total": None if sentinel else total,
        "total_sentinel": sentinel,
        "source": "filter",
        "items": items,
    }


async def _catalog_from_home(keywords: list[str], page: int, host: str | None = DEFAULT_HOST, per_page: int = 24) -> dict | None:
    ops = await catalog_worker.get_home_operating_list(host)
    matched_items: list[dict] = []
    matched_sections: set[str] = set()
    for op in ops:
        op_type = op.get("type")
        title = op.get("title", "")
        if not any(kw.lower() in title.lower() for kw in keywords):
            continue
        if op_type not in ("SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"):
            continue
        matched_sections.add(title)
        for raw in op.get("subjects", []) or []:
            item = normalize_subject_item(raw)
            if item and item.get("subject_id"):
                matched_items.append(item)
    if not matched_items:
        return None
    start = (page - 1) * per_page
    paged = matched_items[start:start + per_page]
    return {
        "page": page,
        "per_page": per_page,
        "total": len(matched_items),
        "total_sentinel": False,
        "source": "home_fallback",
        "host": host or DEFAULT_HOST,
        "sections": sorted(matched_sections),
        "items": paged,
    }


async def _catalog(kind: str, tab_id: int, page: int, sort: str, source: str, host: str) -> dict:
    if source in ("auto", "filter"):
        result = await _get_category_data(tab_id, page=page, sort=sort)
        if not result["total_sentinel"] or source == "filter":
            return result
    fallback = await _catalog_from_home(CATALOG_SECTIONS[kind], page=page, host=host)
    if fallback:
        return fallback
    result = await _get_category_data(tab_id, page=page, sort=sort)
    result["source"] = "filter_sentinel_no_fallback"
    result["note"] = (
        f"No home sections matched for '{kind}'. "
        f"Try /sections to list available sections, /browse/{{section}}, or /catalog."
    )
    return result


@router.get("/movies")
async def get_movies(page: int = 1, sort: str = "RECOMMEND", tab: int | None = None,
                     source: str = "auto", host: str = DEFAULT_HOST):
    return await _catalog("movies", tab if tab is not None else TAB_MOVIES,
                          page, sort, source, host)


@router.get("/tv-series")
async def get_tv_series(page: int = 1, sort: str = "RECOMMEND", tab: int | None = None,
                        source: str = "auto", host: str = DEFAULT_HOST):
    return await _catalog("tv-series", tab if tab is not None else TAB_TV,
                          page, sort, source, host)


@router.get("/animation")
async def get_animation(page: int = 1, sort: str = "RECOMMEND", tab: int | None = None,
                        source: str = "auto", host: str = DEFAULT_HOST):
    return await _catalog("animation", tab if tab is not None else TAB_ANIMATION,
                          page, sort, source, host)


# ─── SEARCH ───────────────────────────────────────────────────────────────────

@router.get("/search/suggest")
async def get_search_suggestions(
    q: str = Query(..., min_length=1),
    enrich: bool = False,
    host: str = DEFAULT_HOST,
):
    data = await upstream.bff_request(
        "/subject/search-suggest", method="POST",
        payload={"keyword": q, "perPage": 10}, preferred_host=host,
    )
    inner = data.get("data", {}) or {}
    raw = inner.get("items") or inner.get("list") or []
    titles: list[str] = []
    for item in raw:
        title = (
            (item.get("subject") or {}).get("title")
            or item.get("word")
            or item.get("title")
            or item.get("keyword")
        )
        if title and title not in titles:
            titles.append(title)

    if enrich:
        suggestions = await asyncio.gather(*(_enrich_suggestion(t, host) for t in titles))
    else:
        suggestions = [{"title": t} for t in titles]

    return {"query": q, "host": host, "count": len(suggestions), "suggestions": suggestions}


async def _enrich_suggestion(title: str, host: str) -> dict:
    try:
        data = await upstream.bff_request(
            "/subject/search", method="POST",
            payload={"keyword": title, "page": 1, "perPage": 1},
            preferred_host=host,
        )
        inner = data.get("data", {}) or {}
        raw = inner.get("items") or inner.get("list") or []
        if raw:
            sub = raw[0]
            return {
                "title": sub.get("title") or title,
                "slug": sub.get("detailPath"),
                "subject_id": sub.get("subjectId"),
            }
    except Exception:
        pass
    return {"title": title, "slug": None, "subject_id": None}


@router.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1, host: str = DEFAULT_HOST):
    data = await upstream.bff_request(
        "/subject/search", method="POST",
        payload={"keyword": q, "page": page, "perPage": 20},
        preferred_host=host,
    )
    inner = data.get("data", {}) or {}
    raw = inner.get("items") or inner.get("list") or []
    items = [
        {
            "name": sub.get("title"),
            "poster_url": (sub.get("cover") or {}).get("url"),
            "slug": sub.get("detailPath"),
            "subject_id": sub.get("subjectId"),
        }
        for sub in raw
    ]
    pager = inner.get("pager", {}) or {}
    total = pager.get("totalCount") or inner.get("total") or len(items)
    return {"query": q, "host": host, "page": page, "total": total, "items": items}


@router.get("/detail/{slug}")
async def get_movie_detail(slug: str, host: str = DEFAULT_HOST):
    return await upstream.bff_request(f"/detail?detailPath={slug}", preferred_host=host)


# ─── STREAM ───────────────────────────────────────────────────────────────────

@router.get("/api/stream/{subject_id}")
async def get_stream_sources(subject_id: str, detail_path: str, se: int = 1, ep: int = 1):
    stream = await upstream.resolve_stream(subject_id, detail_path, se, ep)
    data = stream["data"]
    has_resource = data.get("hasResource", False)
    streams = [
        {
            "resolution": f"{s.get('resolutions')}p" if s.get("resolutions") else None,
            "format": s.get("format"),
            "url": s.get("url"),
            "size": s.get("size"),
            "duration": s.get("duration"),
            "codec": s.get("codecName"),
        }
        for s in data.get("streams", []) or []
    ]
    return {
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "has_resource": has_resource,
        "sources": streams,
        "hls": data.get("hls", []) or [],
        "dash": data.get("dash", []) or [],
        "free_episodes": data.get("freeNum"),
        "limited": data.get("limited", False),
        "note": None if has_resource else "No stream found for this episode.",
    }


@router.get("/api/stream/{subject_id}/captions")
async def get_captions(subject_id: str, detail_path: str, se: int = 1, ep: int = 1):
    captions = await upstream.resolve_captions(subject_id, detail_path, se, ep)
    return {
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "count": len(captions),
        "captions": captions,
    }


@router.get("/api/stream/{subject_id}/proxy")
async def proxy_stream(
    subject_id: str,
    request: Request,
    detail_path: str,
    se: int = 1,
    ep: int = 1,
    res: str = "best",
):
    stream = await upstream.resolve_stream(subject_id, detail_path, se, ep)
    data = stream["data"]
    picked = pick_mp4_stream(data.get("streams") or [], res)
    if not picked:
        raise HTTPException(502, detail=f"No MP4 source for res={res}.")
    stream_url = picked.get("url")
    if not stream_url:
        raise HTTPException(502, detail="Selected source has no URL")

    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Referer": "https://moviebox.ph/",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }
    range_header = request.headers.get("range")
    if range_header:
        req_headers["Range"] = range_header

    client = httpx.AsyncClient(http2=True, follow_redirects=True, timeout=60.0)
    upstream_req = client.build_request("GET", stream_url, headers=req_headers)
    upstream_resp = await client.send(upstream_req, stream=True)
    if upstream_resp.status_code >= 400:
        await upstream_resp.aclose()
        await client.aclose()
        raise HTTPException(502, detail=f"Upstream stream error: {upstream_resp.status_code}")

    async def body_iter():
        try:
            async for chunk in upstream_resp.aiter_bytes(64 * 1024):
                yield chunk
        finally:
            await upstream_resp.aclose()
            await client.aclose()

    fwd_headers = {}
    for h in ("content-length", "content-type", "accept-ranges", "content-range", "etag", "last-modified"):
        if h in upstream_resp.headers:
            fwd_headers[h] = upstream_resp.headers[h]

    return StreamingResponse(
        body_iter(),
        status_code=upstream_resp.status_code,
        headers=fwd_headers,
        media_type=upstream_resp.headers.get("content-type", "video/mp4"),
    )


@router.get("/api/stream/{subject_id}/subtitles/{lang}")
async def get_subtitles_vtt(subject_id: str, lang: str, detail_path: str, se: int = 1, ep: int = 1):
    captions = await upstream.resolve_captions(subject_id, detail_path, se, ep)
    match = next((c for c in captions if (c.get("lan") or "").lower() == lang.lower()), None)
    if not match:
        raise HTTPException(404, detail=f"No caption for language: {lang}")
    srt_url = match.get("url")
    if not srt_url or not srt_url.startswith("https://"):
        raise HTTPException(502, detail="Invalid caption URL")
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        r = await c.get(srt_url)
    if r.status_code != 200:
        raise HTTPException(502, detail=f"Caption fetch failed: {r.status_code}")
    vtt = srt_to_vtt(r.text)
    return Response(
        content=vtt,
        media_type="text/vtt",
        headers={"Content-Disposition": f'inline; filename="{lang}.vtt"'},
    )


# ─── DIAGNOSTICS ──────────────────────────────────────────────────────────────

@router.get("/api/tabs")
async def discover_tabs(min_id: int = 1, max_id: int = 20, per_page: int = 3):
    results = []
    for tab_id in range(min_id, max_id + 1):
        payload = {"tabId": tab_id, "page": 1, "perPage": per_page,
                   **TAB_PAYLOAD_VARIANTS["baseline"]}
        try:
            data = await upstream.bff_request("/subject/filter", method="POST", payload=payload)
            inner = data.get("data", {}) or {}
            raw = inner.get("items") or inner.get("subjects") or []
            pager = inner.get("pager") or {}
            results.append({
                "tab_id": tab_id,
                "count": len(raw),
                "pager_total": pager.get("totalCount"),
                "sentinel": bool((pager.get("totalCount") or 0) >= 1000000),
                "sample_titles": [s.get("title") for s in raw if s.get("title")][:3],
            })
        except Exception as e:
            results.append({"tab_id": tab_id, "error": str(e)})
    return {"variant": "baseline", "results": results}


@router.get("/api/tabs/variants")
async def discover_tab_variants(tab_id: int = 2, per_page: int = 3):
    out = []
    for name, body in TAB_PAYLOAD_VARIANTS.items():
        payload = {"tabId": tab_id, "page": 1, "perPage": per_page}
        if body is not None:
            payload.update(body)
        try:
            data = await upstream.bff_request("/subject/filter", method="POST", payload=payload)
            inner = data.get("data", {}) or {}
            raw = inner.get("items") or inner.get("subjects") or []
            pager = inner.get("pager") or {}
            total = pager.get("totalCount")
            out.append({
                "variant": name, "payload": body, "count": len(raw),
                "pager_total": total, "sentinel": bool((total or 0) >= 1000000),
                "sample_titles": [s.get("title") for s in raw if s.get("title")][:3],
            })
        except Exception as e:
            out.append({"variant": name, "error": str(e)})
    breaking = [v for v in out if not v.get("sentinel") and not v.get("error")]
    return {
        "tab_id": tab_id,
        "variants": out,
        "non_sentinel_variants": [v["variant"] for v in breaking],
        "hint": (
            "If non_sentinel_variants is non-empty, that payload shape works."
        ) if breaking else (
            "All variants sentinel. /subject/filter requires auth we don't have as guest."
        ),
    }


@router.get("/api/debug/upstream")
async def debug_upstream(path: str, method: str = "GET", payload: str | None = None):
    if not path.startswith("/"):
        path = "/" + path
    body = json.loads(payload) if payload else None
    client = state.require_client()
    token = await upstream.get_bearer_token(DEFAULT_HOST)
    headers = {**DEFAULT_HEADERS}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        if method == "POST":
            resp = await client.post(f"{API_BASE}{path}", headers=headers, json=body)
        else:
            resp = await client.get(f"{API_BASE}{path}", headers=headers)
        return resp.json()
    except Exception as e:
        raise HTTPException(502, detail=f"debug upstream failed: {e}")


@router.get("/health")
async def health():
    now = time.time()
    providers_health = {
        h: {
            "alive": st["alive"],
            "latency_ms": int(st["avg_latency_ms"]),
            "fails": st["consecutive_failures"],
            "circuit_open": st["circuit_open_until"] > now,
        }
        for h, st in providers.provider_state.items()
    }
    pool = state.client._transport._pool if state.client is not None else None
    return {
        "status": "ok",
        "version": API_VERSION,
        "default_host": DEFAULT_HOST,
        "known_hosts": KNOWN_HOSTS,
        "providers": providers_health,
        "tokens_cached": len(state.bearer_tokens),
        "domain_cached": state.domain_cache["value"],
        "domain_expires_in": max(0, int(state.domain_cache["expires"] - now)),
        "client_pool_connections": len(pool.connections) if pool is not None else None,
        "catalog_indexed": storage.read_scalar("SELECT COUNT(*) FROM subjects") or 0,
        "walk_running": state.walk_state.get("running", False),
        "enriching": state.walk_state.get("enriching", False),
    }
