"""
betclic_fast.py
===============
Phase 1 — MAPPING  : arborescence complète (sports → compétitions → marchés → cotes)
Phase 2 — SCRAPING : toutes les cotes, tous sports, vitesse maximale (asyncio)

Endpoint : gRPC-Web  https://offering.begmedia.com
Protocole : protobuf encodé/décodé manuellement (pas besoin de .proto)

Structure gRPC confirmée par inspection :
  Listing  top[f1][f3][] = matches
             match[f2]=name  match[f3]=date  match[f8][f2]=competition
             match[f9][f16][] = moneyline sels  (f10=name, f12=odds)
  Detail   top[f1][f1] = inner
             inner[f2]=name  inner[f3]=date  inner[f8][f2]=competition
             inner[f11][f3][] = markets
               market[f2 or f3]=name
               market[f16][]=direct sels  (f10=name, f12=odds)
               market[f10]→[f1]→[f1] = line sels   (f10=name, f12=odds)
"""

import asyncio
import json
import os
import random
import struct
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ──────────────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────────────

GRPC_BASE     = "https://offering.begmedia.com/web/offering.access.api"
LIST_URL      = f"{GRPC_BASE}/offering.access.api.MatchService/GetMatchesBySportWithNotifications"
DETAIL_URL    = f"{GRPC_BASE}/offering.access.api.MatchService/GetMatchWithNotification"

GRPC_HEADERS = {
    "Content-Type":             "application/grpc-web+proto",
    "X-Grpc-Web":               "1",
    "Accept-Language":          "fr-FR,fr;q=0.9",
    "Referer":                  "https://www.betclic.fr/",
    "Origin":                   "https://www.betclic.fr",
    "User-Agent":               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "ngsw-bypass":              "1",
    "x-bg-ref-brand":           "BETCLIC",
    "x-bg-ref-platform":        "DESKTOP",
    "x-bg-ref-regulator-zone":  "FR",
    "x-bg-regulation":          "FR",
}

SPORTS = {
    "football":   "Football",
    "tennis":     "Tennis",
    "basketball": "Basketball",
    "ice_hockey": "Hockey",
    "baseball":   "Baseball",
}

LIST_TIMEOUT    = 18    # streaming — on accumule jusqu'au timeout
DETAIL_TIMEOUT  = 10    # par match
MAX_CONCURRENT  = 40    # requêtes détail simultanées — réduit pour éviter le WAF CloudFront
OUTPUT_FILE     = os.environ.get("BETCLIC_OUTPUT_FILE", "betclic_odds.json")

# Résilience
LIST_RETRIES    = 4     # tentatives max pour un listing sport
DETAIL_RETRIES  = 3     # tentatives max pour un détail match
RETRY_BACKOFF   = [0, 2, 5, 10]    # délais (s) entre tentatives normales
BACKOFF_403     = [10, 30, 60]      # délais (s) spécifiques au 403 (rate-limit WAF)


# ──────────────────────────────────────────────────────────────────────
# Encodeur protobuf minimal
# ──────────────────────────────────────────────────────────────────────

def _varint(v: int) -> bytes:
    r = bytearray()
    while v > 127:
        r.append((v & 0x7F) | 0x80)
        v >>= 7
    r.append(v)
    return bytes(r)

def _sf(field: int, value: str) -> bytes:
    """String field."""
    tag = (field << 3) | 2
    enc = value.encode("utf-8")
    return bytes([tag]) + _varint(len(enc)) + enc

def _vf(field: int, value: int) -> bytes:
    """Varint field."""
    return bytes([(field << 3) | 0]) + _varint(value)

def _frame(payload: bytes) -> bytes:
    return b"\x00" + struct.pack(">I", len(payload)) + payload


# ──────────────────────────────────────────────────────────────────────
# Décodeur protobuf générique
# ──────────────────────────────────────────────────────────────────────

def _dv(data: bytes, pos: int) -> Tuple[int, int]:
    result = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    return result, pos


def _dm(data: bytes, depth: int = 0) -> list:
    """Decode raw protobuf → [(field_num, type, value), ...]"""
    out = []; pos = 0
    while pos < len(data):
        try:
            tag, pos = _dv(data, pos)
            fn, wt = tag >> 3, tag & 7
            if fn == 0: break
            if wt == 0:
                v, pos = _dv(data, pos); out.append((fn, "V", v))
            elif wt == 1:
                if pos + 8 > len(data): break
                v = struct.unpack("<d", data[pos:pos+8])[0]; pos += 8
                out.append((fn, "D", v))
            elif wt == 2:
                ln, pos = _dv(data, pos)
                if pos + ln > len(data): break
                raw = data[pos:pos+ln]; pos += ln
                try:
                    text = raw.decode("utf-8")
                    if "\x00" not in text and all(c.isprintable() or c in "\n\r\t" for c in text):
                        out.append((fn, "S", text)); continue
                except Exception: pass
                if depth < 12:
                    try:
                        sub = _dm(raw, depth + 1)
                        if sub: out.append((fn, "M", sub)); continue
                    except Exception: pass
                out.append((fn, "B", raw))
            elif wt == 5:
                if pos + 4 > len(data): break
                v = struct.unpack("<f", data[pos:pos+4])[0]; pos += 4
                out.append((fn, "F", v))
            else: break
        except Exception: break
    return out


def _g(fields: list, fn: int, ft: str = None):
    """First matching field."""
    for f, t, v in fields:
        if f == fn and (ft is None or t == ft): return v
    return None

def _ga(fields: list, fn: int, ft: str = None) -> list:
    """All matching fields."""
    return [v for f, t, v in fields if f == fn and (ft is None or t == ft)]

def _gs(msg: list, fn: int) -> str:
    """Get string field (fallback to bytes-decoded)."""
    s = _g(msg, fn, "S")
    if s: return s
    b = _g(msg, fn, "B")
    return b.decode("utf-8", "replace") if isinstance(b, bytes) else ""


# ──────────────────────────────────────────────────────────────────────
# Parsing gRPC frames
# ──────────────────────────────────────────────────────────────────────

def _frames(data: bytes) -> List[bytes]:
    out = []; pos = 0
    while pos + 5 <= len(data):
        flag = data[pos]
        ln   = struct.unpack(">I", data[pos+1:pos+5])[0]
        if pos + 5 + ln > len(data): break
        if flag == 0: out.append(data[pos+5:pos+5+ln])
        pos += 5 + ln
    return out


# ──────────────────────────────────────────────────────────────────────
# Extraction des sélections d'un marché (deux patterns possibles)
# ──────────────────────────────────────────────────────────────────────

def _extract_sels(mkt: list) -> List[Tuple[str, float]]:
    """Retourne [(sel_name, odds), ...] pour un bloc marché."""
    results = []
    seen    = set()

    def _add(name: str, odds: float):
        if name and odds and odds > 1.0:
            k = (name, round(odds, 3))
            if k not in seen:
                seen.add(k)
                results.append((name, round(odds, 3)))

    # Pattern A : sélections directes sous f16
    for sel in _ga(mkt, 16, "M"):
        sn = _gs(sel, 10) or _gs(sel, 11)
        ov = _g(sel, 12, "D")
        _add(sn, ov)

    # Pattern B : groupes de lignes sous f10 → f1 → f1 (over/under, etc.)
    for lg in _ga(mkt, 10, "M"):
        for line in _ga(lg, 1, "M"):
            for sel in _ga(line, 1, "M"):
                sn = _gs(sel, 10) or _gs(sel, 11)
                ov = _g(sel, 12, "D")
                _add(sn, ov)

    return results


# ──────────────────────────────────────────────────────────────────────
# Parse listing  (GetMatchesBySportWithNotifications)
# ──────────────────────────────────────────────────────────────────────

def parse_listing(data: bytes, sport_key: str) -> Tuple[List[dict], List[dict]]:
    """
    Retourne :
      rows  = [{"sport","competition","match","date","market","selection","odds","match_id"}, ...]
      metas = [{"mid","match","comp","date","home","away","sport"}, ...]
    """
    rows: List[dict] = []
    metas: List[dict] = []
    seen_ids: set = set()

    for fp in _frames(data):
        top     = _dm(fp)
        wrapper = _g(top, 1, "M")
        if not wrapper: continue

        for mf in _ga(wrapper, 3, "M"):
            mid  = _g(mf, 1, "V")
            name = _gs(mf, 2)
            date = _gs(mf, 3)

            # Compétition : f8.f2
            comp_msg = _g(mf, 8, "M")
            comp = _gs(comp_msg, 2) if comp_msg else ""

            # Équipes : f12[].f3
            contestants = _ga(mf, 12, "M")
            home = _gs(contestants[0], 3) if len(contestants) > 0 else ""
            away = _gs(contestants[1], 3) if len(contestants) > 1 else ""

            # Moneyline listing : f9.f16[].{f10, f12}
            mkt_msg = _g(mf, 9, "M")
            if mkt_msg:
                mkt_name = _gs(mkt_msg, 2) or _gs(mkt_msg, 3) or "moneyline"
                for sel_name, odds in _extract_sels(mkt_msg):
                    rows.append({
                        "sport": sport_key, "competition": comp,
                        "match": name, "date": date,
                        "market": mkt_name, "selection": sel_name,
                        "odds": odds, "match_id": mid,
                    })

            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                metas.append({
                    "mid": mid, "match": name, "comp": comp,
                    "date": date, "home": home, "away": away,
                    "sport": sport_key,
                })

    return rows, metas


# ──────────────────────────────────────────────────────────────────────
# Parse détail d'un match (GetMatchWithNotification)
# ──────────────────────────────────────────────────────────────────────

def parse_detail(data: bytes, meta: dict) -> List[dict]:
    """Parse tous les marchés d'un match depuis la réponse de détail."""
    rows: List[dict] = []
    seen_mkt_sels: set = set()

    for fp in _frames(data):
        top   = _dm(fp)
        outer = _g(top, 1, "M")
        if not outer: continue

        inner = _g(outer, 1, "M")
        if not inner: continue

        # Nom du match et compétition depuis le détail (plus fiable)
        detail_name = _gs(inner, 2) or meta["match"]
        detail_date = _gs(inner, 3) or meta["date"]
        comp_msg    = _g(inner, 8, "M")
        competition = _gs(comp_msg, 2) if comp_msg else meta["comp"]

        # Tous les groupes de marchés : f11[]
        for mkt_grp in _ga(inner, 11, "M"):
            # Chaque marché : f3[]
            for mkt in _ga(mkt_grp, 3, "M"):
                mkt_name = _gs(mkt, 2) or _gs(mkt, 3)
                if not mkt_name: continue

                for sel_name, odds in _extract_sels(mkt):
                    k = (mkt_name, sel_name)
                    if k in seen_mkt_sels: continue
                    seen_mkt_sels.add(k)
                    rows.append({
                        "sport":       meta["sport"],
                        "competition": competition,
                        "match":       detail_name,
                        "date":        detail_date,
                        "market":      mkt_name,
                        "selection":   sel_name,
                        "odds":        odds,
                        "match_id":    meta["mid"],
                    })

    return rows


# ──────────────────────────────────────────────────────────────────────
# Client HTTP + Circuit Breaker global
# ──────────────────────────────────────────────────────────────────────

def _new_client() -> httpx.AsyncClient:
    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT + 30,
        max_keepalive_connections=MAX_CONCURRENT,
    )
    return httpx.AsyncClient(limits=limits, http2=False)


class RateLimitedError(Exception):
    pass


class CircuitBreaker:
    """
    Circuit breaker global partagé entre toutes les coroutines.

    CLOSED  → requêtes autorisées (état normal)
    OPEN    → 403/429 détecté → toutes les coroutines attendent
              après cooldown → HALF-OPEN : une seule requête test
    HALF-OPEN → si succès → CLOSED ; si échec → OPEN (cooldown x2)

    Usage : await CB.wait()   # bloque si OPEN
            CB.trip(cooldown) # ouvre le circuit
            CB.close()        # ferme le circuit (succès)
    """

    def __init__(self):
        self._open   = False
        self._event  = asyncio.Event()
        self._event.set()          # set = circuit fermé = requêtes OK
        self._cooldown   = 20      # secondes de pause initiale
        self._max_cool   = 120
        self._half_lock  = asyncio.Lock()
        self._half_open  = False
        self._trip_task: Optional[asyncio.Task] = None

    async def wait(self):
        """Attend que le circuit soit fermé avant de laisser passer."""
        await self._event.wait()

    def is_open(self) -> bool:
        return self._open

    def trip(self, reason: str = ""):
        """Ouvre le circuit (suspend toutes les requêtes)."""
        if self._open:
            return  # déjà ouvert
        self._open = True
        self._event.clear()
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n  🔴 [{ts}] CIRCUIT OUVERT — {reason} — pause {self._cooldown}s")
        if self._trip_task and not self._trip_task.done():
            self._trip_task.cancel()
        self._trip_task = asyncio.create_task(self._auto_reopen())

    async def _auto_reopen(self):
        """Réouvre automatiquement après cooldown."""
        await asyncio.sleep(self._cooldown)
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  🟡 [{ts}] CIRCUIT HALF-OPEN — test d'une requête…")
        self._half_open = True
        self._open = False
        self._event.set()

    def on_success(self):
        """Appeler après un succès quand HALF-OPEN."""
        if self._half_open:
            self._half_open = False
            self._open = False
            self._cooldown = max(20, self._cooldown // 2)  # réduit le cooldown
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  🟢 [{ts}] CIRCUIT FERMÉ — reprise normale")

    def on_fail_half_open(self):
        """Appeler si la requête test échoue → ré-ouvre avec cooldown doublé."""
        self._half_open = False
        self._cooldown = min(self._max_cool, self._cooldown * 2)
        self._open = False  # trip() va re-setter
        self.trip(f"échec half-open, nouveau cooldown={self._cooldown}s")


# Circuit breaker singleton (partagé par toutes les coroutines)
CB = CircuitBreaker()


async def _grpc_once(
    client: httpx.AsyncClient,
    url: str,
    payload: bytes,
    read_timeout: float,
) -> bytes:
    body = _frame(payload)
    t = httpx.Timeout(connect=12.0, read=read_timeout, write=10.0, pool=60.0)
    async with client.stream("POST", url, content=body, headers=GRPC_HEADERS, timeout=t) as r:
        if r.status_code in (403, 429):
            raise RateLimitedError(f"HTTP {r.status_code}")
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        data = b""
        try:
            async for chunk in r.aiter_bytes():
                data += chunk
        except httpx.ReadTimeout:
            pass
        return data


async def _grpc_resilient(
    client: httpx.AsyncClient,
    url: str,
    payload: bytes,
    read_timeout: float,
    retries: int,
    label: str = "",
) -> bytes:
    """
    Appel gRPC résilient :
    1. Attend que le circuit breaker soit fermé
    2. Exécute la requête
    3. Sur 403/429 → déclenche le circuit breaker global (toutes les
       coroutines en cours se mettent en pause automatiquement)
    4. Sur erreur réseau → backoff court + retry local
    5. Retourne b"" seulement si toutes les tentatives échouent
    """
    last_exc: Optional[Exception] = None

    for attempt in range(retries):
        # ── Attendre que le circuit soit fermé (requêtes autorisées) ─
        await CB.wait()

        # ── Jitter anti-rafale sur les retries ───────────────────────
        if attempt > 0:
            delay = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            await asyncio.sleep(delay + random.uniform(0, 0.5))

        try:
            data = await _grpc_once(client, url, payload, read_timeout)

            if data:
                CB.on_success()   # succès après half-open → referme le circuit
                if attempt > 0:
                    print(f"    ↺ OK après {attempt+1} tentative(s) : {label}")
                return data

            last_exc = RuntimeError("réponse vide")

        except RateLimitedError as exc:
            last_exc = exc
            if CB._half_open:
                CB.on_fail_half_open()
            else:
                CB.trip(f"{exc} — {label}")
            # On retente après réouverture du circuit (wait() bloquera)

        except (httpx.ConnectError, httpx.RemoteProtocolError,
                httpx.PoolTimeout, httpx.WriteError) as exc:
            last_exc = exc

        except httpx.ReadTimeout:
            last_exc = httpx.ReadTimeout("timeout sans données")

        except RuntimeError as exc:
            last_exc = exc

        except Exception as exc:
            last_exc = exc

    print(f"    ✗ abandon ({retries} tentatives) : {label} — {last_exc}")
    return b""


# ──────────────────────────────────────────────────────────────────────
# Phase 1 — MAPPING
# ──────────────────────────────────────────────────────────────────────

async def run_mapping(client: httpx.AsyncClient):
    print("\n" + "="*68)
    print("  PHASE 1 — MAPPING COMPLET Betclic")
    print("="*68)

    # Listing de tous les sports en parallèle
    async def fetch_sport_listing(sk: str):
        payload = _sf(1, sk) + _sf(3, "fr") + _vf(5, 500_000)
        return await _grpc_resilient(
            client, LIST_URL, payload, LIST_TIMEOUT,
            retries=LIST_RETRIES, label=f"listing:{sk}",
        )

    results = await asyncio.gather(*[fetch_sport_listing(sk) for sk in SPORTS])

    all_metas: List[dict] = []
    mapping_tree: Dict[str, Any] = {}

    for sk, data in zip(SPORTS.keys(), results):
        label = SPORTS[sk]
        rows, metas = parse_listing(data, sk)
        all_metas.extend(metas)

        comps: Dict[str, set] = defaultdict(set)
        for r in rows:
            comps[r["competition"]].add(r["market"])

        mapping_tree[sk] = {"label": label, "competitions": comps, "n_matches": len(metas)}

        print(f"\n▶ {label} ({sk})")
        print(f"  Matchs       : {len(metas)}")
        print(f"  Compétitions : {len(comps)}")
        for comp, mkts in sorted(comps.items())[:6]:
            print(f"    {comp:<40} {', '.join(sorted(mkts)[:3])}")
        if len(comps) > 6:
            print(f"    … +{len(comps)-6} autres compétitions")

    # Inspection des marchés de détail (3 matchs/sport)
    print("\n" + "-"*68)
    print("  Marchés de détail (sample 3 matchs/sport)…")
    print("-"*68)

    by_sport: Dict[str, list] = defaultdict(list)
    for m in all_metas:
        by_sport[m["sport"]].append(m)
    sample = []
    for sk, lst in by_sport.items():
        sample.extend(lst[:3])

    sem = asyncio.Semaphore(20)

    async def fetch_detail_sample(meta: dict):
        async with sem:
            payload = _vf(1, meta["mid"]) + _sf(2, "fr")
            data = await _grpc_resilient(
                client, DETAIL_URL, payload, DETAIL_TIMEOUT,
                retries=2, label=meta["match"],
            )
            rows = parse_detail(data, meta)
            return meta["sport"], {r["market"] for r in rows}

    detail_results = await asyncio.gather(*[fetch_detail_sample(m) for m in sample])
    mkt_by_sport: Dict[str, set] = defaultdict(set)
    for sk, mkts in detail_results:
        mkt_by_sport[sk].update(mkts)

    print("\n  Marchés détectés par sport :")
    for sk in SPORTS:
        print(f"\n  [{SPORTS[sk]}]")
        for mkt in sorted(mkt_by_sport.get(sk, set())):
            print(f"    • {mkt}")

    total_matches = len(all_metas)
    total_comps   = sum(len(v["competitions"]) for v in mapping_tree.values())
    print("\n" + "="*68)
    print(f"  Sports       : {len(SPORTS)}")
    print(f"  Compétitions : {total_comps}")
    print(f"  Matchs       : {total_matches}")
    print("="*68)

    return all_metas


# ──────────────────────────────────────────────────────────────────────
# Phase 2 — SCRAPING ULTRA-RAPIDE
# ──────────────────────────────────────────────────────────────────────

async def run_scraping(client: httpx.AsyncClient) -> List[dict]:
    print("\n" + "="*68)
    print("  PHASE 2 — SCRAPING toutes les cotes (vitesse maximale)")
    print("="*68)

    t0 = time.perf_counter()

    # ── Tier 1 : listing de tous les sports en parallèle ──────────────
    print("\n  [1/2] Listing tous sports en parallèle…")

    async def fetch_listing(sk: str):
        payload = _sf(1, sk) + _sf(3, "fr") + _vf(5, 500_000)
        data = await _grpc_resilient(
            client, LIST_URL, payload, LIST_TIMEOUT,
            retries=LIST_RETRIES, label=f"listing:{sk}",
        )
        return parse_listing(data, sk)

    listing_results = await asyncio.gather(*[fetch_listing(sk) for sk in SPORTS])

    all_rows:  List[dict] = []
    all_metas: List[dict] = []

    for sk, (rows, metas) in zip(SPORTS.keys(), listing_results):
        all_rows.extend(rows)
        all_metas.extend(metas)
        print(f"    ✓ {SPORTS[sk]:<12} {len(metas):4} matchs  |  {len(rows):5} cotes listing")

    t1 = time.perf_counter()
    print(f"\n  Listing : {t1-t0:.2f}s  ({len(all_metas)} matchs, {len(all_rows)} cotes tier-1)")

    # ── Tier 2 : détail de tous les matchs en parallèle (asyncio) ─────
    print(f"\n  [2/2] Détails ({len(all_metas)} matchs, concurrence={MAX_CONCURRENT})…")

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    done_count = 0
    lock = asyncio.Lock()

    async def fetch_detail(meta: dict) -> List[dict]:
        nonlocal done_count
        async with sem:
            payload = _vf(1, meta["mid"]) + _sf(2, "fr")
            data = await _grpc_resilient(
                client, DETAIL_URL, payload, DETAIL_TIMEOUT,
                retries=DETAIL_RETRIES, label=meta["match"],
            )
            result = parse_detail(data, meta)
            async with lock:
                done_count += 1
                n = done_count
            if n % 50 == 0 or n == len(all_metas):
                elapsed = time.perf_counter() - t1
                print(f"    {n}/{len(all_metas)} traités  ({elapsed:.1f}s)")
            return result

    detail_batches = await asyncio.gather(*[fetch_detail(m) for m in all_metas])
    for batch in detail_batches:
        all_rows.extend(batch)

    t2 = time.perf_counter()
    print(f"\n  Détails : {t2-t1:.2f}s")
    print(f"\n  ⏱  TEMPS TOTAL   : {t2-t0:.2f}s")
    print(f"  📊 COTES TOTALES : {len(all_rows)}")

    return all_rows


# ──────────────────────────────────────────────────────────────────────
# Résumé & export JSON
# ──────────────────────────────────────────────────────────────────────

def display_summary(rows: List[dict]):
    print("\n" + "="*68)
    print("  RÉSUMÉ PAR SPORT")
    print("="*68)

    by_sport: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    comps_seen:  set = set()
    matches_seen: set = set()

    for r in rows:
        by_sport[r["sport"]][r["market"]] += 1
        comps_seen.add((r["sport"], r["competition"]))
        matches_seen.add(r["match_id"])

    for sk in SPORTS:
        mkts = by_sport.get(sk, {})
        total = sum(mkts.values())
        n_comp = sum(1 for s, _ in comps_seen if s == sk)
        n_match = sum(1 for r in rows if r["sport"] == sk and r["match_id"])
        n_match_uniq = len({r["match_id"] for r in rows if r["sport"] == sk})
        print(f"\n  [{SPORTS[sk]}]  {total} cotes  |  {n_match_uniq} matchs  |  {n_comp} compétitions")
        for mkt, cnt in sorted(mkts.items(), key=lambda x: -x[1])[:8]:
            print(f"    {cnt:5}  {mkt}")
        if len(mkts) > 8:
            print(f"    … +{len(mkts)-8} autres marchés")

    print(f"\n  Compétitions uniques : {len(comps_seen)}")
    print(f"  Matchs uniques       : {len(matches_seen)}")
    print(f"  Lignes totales       : {len(rows)}")


def save_json(rows: List[dict], path: str = OUTPUT_FILE):
    # Arborescence : sport → compétition → match → cotes
    tree: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for r in rows:
        sp   = SPORTS.get(r["sport"], r["sport"])
        comp = r["competition"] or "Autres"
        name = r["match"]
        key  = f"{name}::{r['date']}"

        if sp not in tree: tree[sp] = {}
        if comp not in tree[sp]: tree[sp][comp] = {}
        if key not in tree[sp][comp]:
            tree[sp][comp][key] = {
                "match":    name,
                "match_id": r["match_id"],
                "date":     r["date"],
                "odds":     [],
            }
        tree[sp][comp][key]["odds"].append({
            "market":    r["market"],
            "selection": r["selection"],
            "odds":      r["odds"],
        })

    # Sérialisation finale
    out: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_odds":   len(rows),
        "sports": {},
    }
    for sp, comps in tree.items():
        out["sports"][sp] = {}
        for comp, matches in sorted(comps.items()):
            out["sports"][sp][comp] = sorted(
                list(matches.values()),
                key=lambda m: m["date"],
            )

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)
    print(f"\n  ✅ Sauvegardé → {path}  ({len(rows)} cotes)")


# ──────────────────────────────────────────────────────────────────────
# Mode loop continu
# ──────────────────────────────────────────────────────────────────────

async def run_loop(interval: int = 90):
    """
    Scrape en permanence toutes les `interval` secondes.
    - Réutilise le même AsyncClient (keep-alive, pas de reconnexion)
    - Circuit breaker global gère les 403 sans interrompre la boucle
    - Affiche les delta (nouvelles cotes, cotes modifiées) à chaque cycle
    """
    print(f"\n  MODE LOOP — refresh toutes les {interval}s  (Ctrl+C pour arrêter)")
    print("="*68)

    prev_odds: Dict[str, float] = {}   # clé → odds du cycle précédent
    cycle = 0

    async with _new_client() as client:
        while True:
            cycle += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n  ┌─ CYCLE {cycle}  [{ts}]")
            t0 = time.perf_counter()

            try:
                rows = await run_scraping(client)
            except Exception as exc:
                print(f"  ✗ Erreur cycle {cycle} : {exc}")
                await asyncio.sleep(interval)
                continue

            # ── Delta (nouvelles / modifiées) ─────────────────────
            new_curr: Dict[str, float] = {}
            added = changed = 0
            for r in rows:
                k = f"{r['sport']}|{r['match']}|{r['market']}|{r['selection']}"
                new_curr[k] = r["odds"]
                if k not in prev_odds:
                    added += 1
                elif abs(prev_odds[k] - r["odds"]) > 0.001:
                    changed += 1
            prev_odds = new_curr

            elapsed = time.perf_counter() - t0
            save_json(rows)
            print(f"  └─ Cycle {cycle} terminé en {elapsed:.1f}s  "
                  f"| {len(rows)} cotes  "
                  f"| +{added} nouvelles  ~{changed} modifiées")

            # ── Attendre jusqu'au prochain cycle ──────────────────
            wait = max(0, interval - elapsed)
            if wait > 0:
                print(f"  … prochain cycle dans {wait:.0f}s")
                await asyncio.sleep(wait)


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée async principal
# ──────────────────────────────────────────────────────────────────────

async def main():
    args = sys.argv[1:]
    # CLI prioritaire ; sinon variables d’environnement (Railway / VPS)
    mode = args[0] if args else os.environ.get("BETCLIC_MODE", "all")

    if mode == "loop":
        if len(args) > 1:
            interval = int(args[1])
        else:
            interval = int(os.environ.get("BETCLIC_LOOP_INTERVAL", "90"))
        await run_loop(interval)
        return

    async with _new_client() as client:
        if mode in ("map", "all"):
            await run_mapping(client)
        if mode in ("scrape", "all"):
            rows = await run_scraping(client)
            display_summary(rows)
            save_json(rows)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Arrêt demandé.")
