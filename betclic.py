"""Betclic scraper — fetches odds via gRPC-Web from offering.begmedia.com.

Betclic migrated from their REST API (offer.cdn.betclic.fr, now dead) to a
gRPC-Web API at offering.begmedia.com. This scraper makes direct gRPC-Web calls
with raw protobuf encoding/decoding — no .proto schema needed.

The gRPC method GetMatchesBySportWithNotifications is server-streaming:
the server sends match data and keeps the connection open for live updates.
We read with a timeout to collect the initial batch, then close.

No geo-restriction on the gRPC endpoint — works from any IP.

Scope (scraping_specifications_clean.pdf §3.1 « Sports & Markets », prematch only):
only those listed markets — football, tennis, basketball, hockey, NHL/NBA player
props. Mapping targets foot_*, tennis_*, basket_*, hockey_*, prop_nba_*, prop_hockey_*;
see _classify_detail_market. Tier-1 listing is used for match ids / ML; the bulk of
spec-aligned lines comes from the match-detail payload (nested markets).

Lines not offered by Betclic are absent. Unmapped detail labels stay as `{sport}_spec_raw`
with selection `«marché»||«sélection»`. Protobuf strings are UTF-8 for correct FR matching.
"""

import logging
import os
import re
import struct
import time
import unicodedata
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx


from scrapers.league_mapping import map_league, get_unmapped_tournaments, reset_unmapped_tournaments

logger = logging.getLogger(__name__)

# ======================================================================
# Constants (kept for backward compat with betclic_sse.py)
# ======================================================================

# gRPC sport code -> internal sport code
# Note: GetMatchesBySport field 1 must match Betclic’s catalogue id (e.g. ice_hockey, not ice-hockey).
BETCLIC_SPORTS = {
    "football": "FOOT",
    "tennis": "TENNIS",
    "basketball": "BASKET",
    "ice-hockey": "HOCKEY",
    "baseball": "BASEBALL",
}


def _grpc_sport_code_for_request(sport_code: str) -> str:
    """Map our sport key to the string Betclic expects in gRPC field 1."""
    if sport_code == "ice-hockey":
        return "ice_hockey"
    return sport_code

# Legacy competition ID map (kept for SSE listener backward compat)
BETCLIC_COMPETITION_MAP: Dict[int, str] = {
    1: "EPL", 2: "LIGA", 3: "BUND", 4: "SERA", 553: "LIG1",
    5: "UCL", 7: "UEL", 9: "MLS", 6: "EFL",
    32: "BUND2", 33: "LIGA2", 34: "SERB", 35: "LIG2",
    8: "ERED", 31: "PRIM", 36: "JUPIL", 10: "SCOT", 1000: "UECL",
    15: "NBA", 16: "NCAAB", 20: "TENNIS", 25: "NHL", 30: "MLB",
}

# Legacy constants (kept for SSE listener backward compat)
API_BASE = "https://offering.begmedia.com"  # Updated from dead domain
API_PARAMS = {"countrycode": "fr", "language": "fr"}

# Reverse map
_LEAGUE_TO_COMP_ID: Dict[str, int] = {v: k for k, v in BETCLIC_COMPETITION_MAP.items()}

# Draw labels (French)
DRAW_LABELS = {"nul", "match nul", "draw", "x"}

YES_LABELS = frozenset({"oui", "yes", "o", "y"})
NO_LABELS = frozenset({"non", "no", "n"})


def _unaccent(s: str) -> str:
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _norm_key(s: str) -> str:
    return _unaccent(s).lower().strip()


def _team_name_in_market(team_norm: str, mkt_norm: str) -> bool:
    """True if team name appears in market title (avoids e.g. 'a' matching 'nombre')."""
    if not team_norm or len(team_norm) < 2:
        return False
    if len(team_norm) <= 4:
        return bool(re.search(rf"\b{re.escape(team_norm)}\b", mkt_norm))
    return team_norm in mkt_norm


# ======================================================================
# gRPC-Web protocol
# ======================================================================

GRPC_BASE = "https://offering.begmedia.com/web/offering.access.api"
GRPC_MATCH_METHOD = "offering.access.api.MatchService/GetMatchesBySportWithNotifications"
GRPC_HEADERS = {
    "Content-Type": "application/grpc-web+proto",
    "X-Grpc-Web": "1",
    "Accept-Language": "fr-FR",
    "Referer": "https://www.betclic.fr/",
    "ngsw-bypass": "1",
    "x-bg-ref-brand": "BETCLIC",
    "x-bg-ref-platform": "DESKTOP",
    "x-bg-ref-regulator-zone": "FR",
    "x-bg-regulation": "FR",
}

# Read timeout for server-streaming gRPC (seconds).
# The server sends the initial data batch then keeps the connection open.
# We read until this timeout fires, then process what we have.
GRPC_READ_TIMEOUT = 8

# Field 5 in GetMatchesBySport: requested max matches (Betclic API). High default = no practical cap.
GRPC_MATCH_LIST_LIMIT_DEFAULT = 500_000


def _grpc_detail_max_workers() -> int:
    """Parallel detail fetches per sport (I/O bound). Lower if Betclic rate-limits."""
    raw = (
        os.environ.get("BETCLIC_GRPC_DETAIL_WORKERS")
        or os.environ.get("BETCLICK_GRPC_DETAIL_WORKERS", "")
    ).strip()
    if raw:
        try:
            return max(1, min(256, int(raw)))
        except ValueError:
            pass
    return min(128, max(32, (os.cpu_count() or 4) * 12))


# ======================================================================
# Raw protobuf codec (no .proto schema needed)
# ======================================================================

def _pb_encode_varint(value: int) -> bytes:
    """Encode an integer as a protobuf varint (LEB128)."""
    result = bytearray()
    while value > 127:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _pb_encode_string(field: int, value: str) -> bytes:
    """Encode a string field."""
    tag = (field << 3) | 2
    encoded = value.encode("utf-8")
    return bytes([tag]) + _pb_encode_varint(len(encoded)) + encoded


def _pb_encode_varint_field(field: int, value: int) -> bytes:
    """Encode a varint field."""
    tag = (field << 3) | 0
    return bytes([tag]) + _pb_encode_varint(value)


def _grpc_frame(payload: bytes) -> bytes:
    """Wrap protobuf payload in a gRPC-Web frame (5-byte header)."""
    return b"\x00" + struct.pack(">I", len(payload)) + payload


def _pb_decode_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """Decode a varint at offset. Returns (value, new_offset)."""
    result = 0
    shift = 0
    while offset < len(data):
        b = data[offset]
        offset += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, offset
        shift += 7
    return result, offset


def _pb_decode_message(data: bytes, depth: int = 0, max_depth: int = 8) -> list:
    """Decode raw protobuf into list of (field_num, type, value).

    Types: 'V' (varint), 'D' (double), 'F' (float), 'S' (string), 'M' (message), 'B' (bytes).
    """
    results = []
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _pb_decode_varint(data, pos)
            field_num = tag >> 3
            wire_type = tag & 7
            if field_num == 0:
                break
            if wire_type == 0:  # varint
                value, pos = _pb_decode_varint(data, pos)
                results.append((field_num, "V", value))
            elif wire_type == 1:  # 64-bit (double)
                if pos + 8 > len(data):
                    break
                value = struct.unpack("<d", data[pos : pos + 8])[0]
                pos += 8
                results.append((field_num, "D", value))
            elif wire_type == 2:  # length-delimited
                length, pos = _pb_decode_varint(data, pos)
                if pos + length > len(data):
                    break
                value = data[pos : pos + length]
                pos += length
                # Try as UTF-8 string first
                try:
                    text = value.decode("utf-8")
                    if "\x00" not in text and all(
                        c.isprintable() or c in "\n\r\t" for c in text
                    ):
                        results.append((field_num, "S", text))
                        continue
                except (UnicodeDecodeError, ValueError):
                    pass
                # Try as nested message
                if depth < max_depth:
                    try:
                        sub = _pb_decode_message(value, depth + 1, max_depth)
                        if sub:
                            results.append((field_num, "M", sub))
                            continue
                    except Exception:
                        pass
                results.append((field_num, "B", value))
            elif wire_type == 5:  # 32-bit (float)
                if pos + 4 > len(data):
                    break
                value = struct.unpack("<f", data[pos : pos + 4])[0]
                pos += 4
                results.append((field_num, "F", value))
            else:
                break
        except Exception:
            break
    return results


def _pb_get(fields: list, field_num: int, type_filter: str = None):
    """Get first matching field value from decoded protobuf."""
    for fn, ft, fv in fields:
        if fn == field_num and (type_filter is None or ft == type_filter):
            return fv
    return None


def _pb_get_all(fields: list, field_num: int, type_filter: str = None) -> list:
    """Get all matching field values from decoded protobuf."""
    return [fv for fn, ft, fv in fields if fn == field_num and (type_filter is None or ft == type_filter)]


def _pb_string_field(msg: list, field_num: int) -> str:
    """String field may be encoded as UTF-8 bytes in some responses."""
    s = _pb_get(msg, field_num, "S")
    if s:
        return s
    b = _pb_get(msg, field_num, "B")
    if b and isinstance(b, bytes):
        return b.decode("utf-8", errors="replace")
    return ""


def _parse_over_under_line(sel_name: str) -> Optional[Tuple[str, str]]:
    """Parse selection into ('Over'|'Under', line) e.g. ('Over', '2.5')."""
    sel_lower = _norm_key(sel_name)
    m = re.match(r"\+\s*de\s*([\d,]+\.?\d*)", sel_lower)
    if m:
        return "Over", m.group(1).replace(",", ".")
    m = re.match(r"-\s*de\s*([\d,]+\.?\d*)", sel_lower)
    if m:
        return "Under", m.group(1).replace(",", ".")
    m = re.match(r"over\s*([\d.]+)", sel_lower)
    if m:
        return "Over", m.group(1)
    m = re.match(r"under\s*([\d.]+)", sel_lower)
    if m:
        return "Under", m.group(1)
    m = re.match(r"(?:plus\s+de|moins\s+de)\s*([\d.]+)", sel_lower)
    if m:
        side = "Over" if "plus" in sel_lower else "Under"
        return side, m.group(1)
    return None


def _parse_yes_no(sel_name: str) -> Optional[str]:
    t = _norm_key(sel_name)
    if t in YES_LABELS:
        return "Yes"
    if t in NO_LABELS:
        return "No"
    return None


def _foot_period(mkt: str) -> Optional[str]:
    """'ht' | 'ft' | None (unknown)."""
    n = _norm_key(mkt)
    if any(
        x in n
        for x in (
            "mi-temps",
            "mi temps",
            "1ere mi-temps",
            "1ère mi-temps",
            "premiere mi-temps",
            "1st half",
            "first half",
            "avant la pause",
        )
    ):
        return "ht"
    if any(
        x in n
        for x in (
            "fin de match",
            "temps reglementaire",
            "temps réglementaire",
            "match entier",
            "90 minutes",
            "plein temps",
            "full time",
            "regulation",
        )
    ):
        return "ft"
    return None


def _basket_period(mkt: str) -> str:
    n = _norm_key(mkt)
    if (
        "quart-temps 1" in n
        or "quart temps 1" in n
        or " qt 1" in n
        or re.search(r"\bq1\b", n)
        or "1er quart" in n
        or "premier quart" in n
        or "1st quarter" in n
    ):
        return "q1"
    if (
        "quart-temps 2" in n
        or "quart temps 2" in n
        or " qt 2" in n
        or re.search(r"\bq2\b", n)
        or "2e quart" in n
        or "2eme quart" in n
        or "2nd quarter" in n
    ):
        return "q2"
    if (
        "quart-temps 3" in n
        or "quart temps 3" in n
        or re.search(r"\bq3\b", n)
        or "3e quart" in n
        or "3rd quarter" in n
    ):
        return "q3"
    if (
        "quart-temps 4" in n
        or "quart temps 4" in n
        or re.search(r"\bq4\b", n)
        or "4e quart" in n
        or "4th quarter" in n
    ):
        return "q4"
    if "mi-temps" in n or "mi temps" in n or "first half" in n or re.search(r"\bht\b", n):
        return "ht"
    return "ft"


def _hockey_period(mkt: str) -> Optional[str]:
    n = _norm_key(mkt)
    if (
        "prolongation" in n
        or "extra time" in n
        or "overtime" in n
        or "shootout" in n
        or re.search(r"\bet\b", n)
        or "apres prolongation" in n
    ):
        return "et"
    if "temps reglementaire" in n or "temps réglementaire" in n or "regular time" in n or re.search(r"\brt\b", n):
        return "rt"
    if "1ere periode" in n or "1ère période" in n or "periode 1" in n or re.search(r"\bp1\b", n) or "1st period" in n:
        return "p1"
    if "2e periode" in n or "2ème période" in n or "periode 2" in n or re.search(r"\bp2\b", n):
        return "p2"
    if "3e periode" in n or "3ème période" in n or "periode 3" in n or re.search(r"\bp3\b", n):
        return "p3"
    return None


def _tennis_scope(mkt: str) -> Optional[str]:
    n = _norm_key(mkt)
    if any(
        x in n
        for x in (
            "1er set",
            "1ère manche",
            "1st set",
            "set 1",
            "premier set",
            "1ere manche",
            "manche 1",
        )
    ):
        return "s1"
    if any(
        x in n
        for x in (
            "match",
            "gagnant",
            "winner",
            "finale",
            "rencontre",
            "vainqueur du match",
        )
    ):
        return "ft"
    return None


def _spec_raw_key(sport_code: str) -> str:
    """Stable market_type for lines that could not be mapped to a spec bucket."""
    return f"{sport_code.replace('-', '_')}_spec_raw"


def _dash_suggests_player_stat(mkt_name: str) -> bool:
    """True if a ' - ' separates matchup from a stat line, not 'Team A - Team B'."""
    if not re.search(r"\s-\s| – | — ", mkt_name):
        return False
    m = _norm_key(mkt_name)
    if not re.search(r"\b(points|rebonds|passes|assists|pts|reb|pra|double|triple|panier)\b", m, re.I):
        return False
    segments = re.split(r"\s-\s| – | — ", mkt_name)
    if not segments:
        return False
    last = _norm_key(segments[-1])
    return bool(
        re.search(
            r"\b(points|rebonds|passes|assists|total|stats|performance|marqueur)\b",
            last,
            re.I,
        )
    )


def _flatten_odds_leaf_messages(msg: list, depth: int = 0) -> List[list]:
    """Collect leaf selection messages (decoded field lists) with f10/f11 label and f12 odds."""
    out: List[list] = []
    if not msg or depth > 14:
        return out
    odds = _pb_get(msg, 12, "D")
    sn = _pb_string_field(msg, 10) or _pb_string_field(msg, 11)
    if odds and odds > 1.0 and sn:
        out.append(msg)
    for fn, ft, fv in msg:
        if ft == "M":
            out.extend(_flatten_odds_leaf_messages(fv, depth + 1))
    return out


def _iter_market_blocks(fields: list, depth: int = 0):
    """Walk protobuf tree; yield (market_title, selection_messages) for each market node.

    Most markets expose selections under field 1. Others (e.g. « Écart de buts », some totals)
    nest selections under repeated field 10; we flatten those to leaf messages with f10/f11/f12.
    """
    if not fields or depth > 12:
        return
    mkt = _pb_string_field(fields, 3) or _pb_string_field(fields, 2)
    sels = _pb_get_all(fields, 1, "M")
    tens = _pb_get_all(fields, 10, "M")
    skip_f10_children = False
    if mkt and sels:
        yield (mkt, sels)
    elif mkt and tens:
        leaves: List[list] = []
        for t in tens:
            leaves.extend(_flatten_odds_leaf_messages(t))
        if leaves:
            yield (mkt, leaves)
            skip_f10_children = True
    for fn, ft, fv in fields:
        if ft == "M" and not (skip_f10_children and fn == 10):
            yield from _iter_market_blocks(fv, depth + 1)


def _is_nba_player_prop_market(mkt_name: str, sel_name: str = "") -> bool:
    """Betclic NBA props often omit 'joueur' — use title shape + stat words.

    Game/team totals (Over 100+ on points) are excluded unless the title looks
    like a named player market (dash, performance, etc.).
    """
    if not mkt_name or not mkt_name.strip():
        return False
    m = _norm_key(mkt_name)
    raw = mkt_name.strip()
    ou_line = _parse_over_under_line(sel_name)
    if ou_line:
        try:
            line_val = float(ou_line[1].replace(",", "."))
        except ValueError:
            line_val = None
        if line_val is not None and line_val >= 70.0:
            strong = any(
                k in m
                for k in (
                    "joueur",
                    "player",
                    "performance",
                    "statistiques",
                    "statistique",
                    "marqueur",
                    "meilleur marqueur",
                )
            ) or _dash_suggests_player_stat(raw)
            if not strong:
                return False
    if any(
        k in m
        for k in (
            "joueur",
            "player",
            "performance",
            "statistiques",
            "statistique",
            "meilleur marqueur",
            "marqueur",
            "double-double",
            "triple-double",
            "double double",
            "triple double",
            "pra",
            "points +",
            "panier a 3",
            "panier a trois",
            "paniers a 3",
            "passes decisives",
            "interceptions",
            "contres",
        )
    ):
        return True
    if any(
        x in m
        for x in (
            "mi-temps",
            "quart-temps",
            "1er quart",
            "2e quart",
            "3e quart",
            "4e quart",
            "du match",
            "de la rencontre",
            "de l'equipe",
            "equipe a",
            "equipe b",
            "ecart",
            "marge",
            "handicap",
            "spread",
            "combine",
            "score final",
        )
    ):
        return False
    if _dash_suggests_player_stat(raw):
        return True
    return False


# ======================================================================
# Aggregated JSON (output.json: generated_at + sports → competitions → matches)
# ======================================================================

_OUTPUT_SPORT_NAMES = {
    "football": "Football",
    "tennis": "Tennis",
    "basketball": "Basketball",
    "ice-hockey": "Hockey",
    "baseball": "Baseball",
}

_OUTPUT_PERIOD_SUFFIX = {
    "ht": "HT",
    "ft": "FT",
    "q1": "Q1",
    "q2": "Q2",
    "q3": "Q3",
    "q4": "Q4",
    "s1": "S1",
    "p1": "P1",
    "p2": "P2",
    "p3": "P3",
    "rt": "RT",
    "et": "ET",
}

_OUTPUT_PROP_STAT_LABEL = {
    "prop_nba_points": "Total Points",
    "prop_nba_assists": "Total Assists",
    "prop_nba_rebounds": "Total Rebounds",
    "prop_nba_par": "Pts+Reb+Ast",
    "prop_nba_3pm": "Total 3PT",
    "prop_nba_double_double": "Double-Double",
    "prop_nba_triple_double": "Triple-Double",
    "prop_hockey_points": "Total Points",
    "prop_hockey_goals": "Total Goals",
    "prop_hockey_sog": "Shots on Goal",
    "prop_hockey_assists": "Total Assists",
}


def _output_infer_period(market_type: str) -> str:
    for part in reversed(market_type.split("_")):
        if part in _OUTPUT_PERIOD_SUFFIX:
            return _OUTPUT_PERIOD_SUFFIX[part]
    return "FT"


def _output_infer_market_channel(market_type: str) -> str:
    mt = market_type.lower()
    if mt in ("moneyline", "total", "spread"):
        return mt
    if "spread" in mt or "hcp" in mt:
        return "spread"
    if (
        "total" in mt
        or "btts" in mt
        or "team_a_score" in mt
        or "team_b_score" in mt
        or "win_nil" in mt
        or "total_team" in mt
        or "total_goals" in mt
    ):
        return "total"
    return "moneyline"


def _output_is_player_prop_row(market_type: str) -> bool:
    return market_type.startswith("prop_nba_") or market_type.startswith("prop_hockey_")


def _output_parse_player_stat(market_label: str, market_type: str) -> Tuple[str, str]:
    label = (market_label or "").strip()
    if not label:
        return "", _OUTPUT_PROP_STAT_LABEL.get(market_type, market_type)
    parts = re.split(r"\s-\s| – | — ", label, maxsplit=1)
    if len(parts) >= 2 and len(parts[0]) < 80:
        stat = parts[1].strip()
        if not stat:
            stat = _OUTPUT_PROP_STAT_LABEL.get(market_type, market_type)
        return parts[0].strip(), stat
    return "", label


def _output_normalize_date(iso_s: str) -> str:
    if not (iso_s or "").strip():
        return ""
    s = iso_s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return iso_s


def _dedupe_market_lines(entries: List[dict]) -> List[dict]:
    """One line per (market, period, selection); last odds wins."""
    out: Dict[Tuple[str, str, str], dict] = {}
    order: List[Tuple[str, str, str]] = []
    for e in entries:
        k = (e.get("market", ""), e.get("period", ""), e.get("selection", ""))
        if k not in out:
            order.append(k)
        out[k] = e
    return [out[k] for k in order]


def _dedupe_prop_lines(entries: List[dict]) -> List[dict]:
    """One line per (player, stat, selection); last odds wins."""
    out: Dict[Tuple[str, str, str], dict] = {}
    order: List[Tuple[str, str, str]] = []
    for e in entries:
        k = (e.get("player", ""), e.get("stat", ""), e.get("selection", ""))
        if k not in out:
            order.append(k)
        out[k] = e
    return [out[k] for k in order]


def count_duplicate_lines_in_output_json(payload: dict) -> Dict[str, int]:
    """Count duplicate market/prop keys within the same match (should be 0 after dedupe in rows_to_output_structure).

    Returns: {"markets": n, "props": n} where n is the number of extra lines (beyond first) per key.
    """
    extra_markets = 0
    extra_props = 0
    for _sp_name, sdata in (payload or {}).get("sports", {}).items():
        for _comp, matches in (sdata or {}).get("competitions", {}).items():
            for m in matches or []:
                mkeys = [
                    (x.get("market"), x.get("period"), x.get("selection"))
                    for x in (m.get("markets") or [])
                ]
                if mkeys:
                    extra_markets += len(mkeys) - len(set(mkeys))
                pkeys = [
                    (x.get("player"), x.get("stat"), x.get("selection"))
                    for x in (m.get("props") or [])
                ]
                if pkeys:
                    extra_props += len(pkeys) - len(set(pkeys))
    return {"markets": extra_markets, "props": extra_props}


def rows_to_output_structure(rows: List[dict]) -> dict:
    """Group scraper rows into {generated_at, sports: {...}} for output.json."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    tree: Dict[str, Dict[str, Dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))

    for r in rows:
        sc = r.get("sport_code") or ""
        sp_name = _OUTPUT_SPORT_NAMES.get(sc)
        if not sp_name:
            continue
        title = (r.get("title") or "").strip()
        if not title:
            continue
        comp = (r.get("competition") or "").strip() or "Autres"
        starts = _output_normalize_date(r.get("starts_at") or "")
        mk = f"{title}::{starts}"
        if mk not in tree[sp_name][comp]:
            tree[sp_name][comp][mk] = {
                "match": title,
                "date": starts,
                "markets": [],
                "props": [],
            }
        bucket = tree[sp_name][comp][mk]
        mt = r.get("market_type") or ""
        sel = (r.get("selection") or "").strip()
        odds = r.get("odds")
        if odds is None:
            continue
        ml = r.get("market_label") or ""

        if _output_is_player_prop_row(mt):
            player, stat = _output_parse_player_stat(ml, mt)
            if not stat:
                stat = _OUTPUT_PROP_STAT_LABEL.get(mt, mt)
            bucket["props"].append(
                {
                    "player": player or "",
                    "stat": stat,
                    "selection": sel,
                    "odds": odds,
                }
            )
            continue

        period = _output_infer_period(mt)
        channel = _output_infer_market_channel(mt)
        bucket["markets"].append(
            {
                "market": channel,
                "period": period,
                "selection": sel,
                "odds": odds,
            }
        )

    for _sp_name, comps in tree.items():
        for _cname, matches in comps.items():
            for _mk, mdata in matches.items():
                mdata["markets"] = _dedupe_market_lines(mdata["markets"])
                mdata["props"] = _dedupe_prop_lines(mdata["props"])

    sports_out: Dict[str, Any] = {}
    for sp_name, comps in tree.items():
        total_matches = 0
        total_rows = 0
        comp_out: Dict[str, list] = {}
        for cname, matches in comps.items():
            lst = []
            for _k, mdata in sorted(matches.items(), key=lambda x: (x[1]["date"], x[1]["match"])):
                total_matches += 1
                total_rows += len(mdata["markets"]) + len(mdata["props"])
                lst.append(
                    {
                        "match": mdata["match"],
                        "date": mdata["date"],
                        "markets": mdata["markets"],
                        "props": mdata["props"],
                    }
                )
            if lst:
                comp_out[cname] = lst
        sports_out[sp_name] = {
            "total_rows": total_rows,
            "total_matches": total_matches,
            "competitions": comp_out,
        }

    sports_ordered: Dict[str, Any] = {}
    for _code, sp_name in _OUTPUT_SPORT_NAMES.items():
        sports_ordered[sp_name] = sports_out.get(
            sp_name,
            {"total_rows": 0, "total_matches": 0, "competitions": {}},
        )

    return {"generated_at": generated_at, "sports": sports_ordered}


# ======================================================================
# Betclic gRPC scraper
# ======================================================================

class BetclicScraper:
    """Fetch Betclic odds via gRPC-Web and save to bookmaker_odds table."""

    def __init__(self):
        self._client = None

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=httpx.Timeout(
                    connect=10.0, read=GRPC_READ_TIMEOUT, write=10.0, pool=60.0
                ),
                http2=False,
                limits=httpx.Limits(max_connections=256, max_keepalive_connections=128),
            )
        return self._client

    def _stream_grpc(self, url: str, payload: bytes, read_timeout: int = GRPC_READ_TIMEOUT) -> bytes:
        """Make a streaming gRPC-Web call and return accumulated bytes."""
        request_body = _grpc_frame(payload)
        client = self._get_client()
        req_timeout = httpx.Timeout(
            connect=10.0, read=float(read_timeout), write=10.0, pool=60.0
        )
        try:
            with client.stream(
                "POST",
                url,
                content=request_body,
                headers=GRPC_HEADERS,
                timeout=req_timeout,
            ) as response:
                if response.status_code != 200:
                    return b""
                data = b""
                try:
                    for chunk in response.iter_bytes():
                        data += chunk
                except httpx.ReadTimeout:
                    pass
                return data
        except Exception:
            return b""

    def _parse_grpc_frames(self, data: bytes) -> List[bytes]:
        """Extract data frame payloads from gRPC-Web response."""
        frames = []
        pos = 0
        while pos + 5 <= len(data):
            flag = data[pos]
            frame_len = struct.unpack(">I", data[pos + 1 : pos + 5])[0]
            if pos + 5 + frame_len > len(data):
                break
            if flag == 0:
                frames.append(data[pos + 5 : pos + 5 + frame_len])
            pos += 5 + frame_len
        return frames

    def _fetch_sport_grpc(
        self, sport_code: str, limit: int = GRPC_MATCH_LIST_LIMIT_DEFAULT
    ) -> List[dict]:
        """Fetch all matches for a sport via gRPC-Web.

        Two-tier approach:
        1. GetMatchesBySport — returns all matches with moneyline (1X2) odds
        2. GetMatchWithNotification (parallel) — returns detailed markets
           (totals, double chance, etc.) for each match

        Returns list of row dicts ready for DataFrame.
        """
        internal_code = BETCLIC_SPORTS.get(sport_code, "UNKNOWN")

        # --- Tier 1: Sport listing (moneyline odds) ---
        grpc_sport = _grpc_sport_code_for_request(sport_code)
        payload = (
            _pb_encode_string(1, grpc_sport)
            + _pb_encode_string(3, "fr")
            + _pb_encode_varint_field(5, limit)
        )
        url = f"{GRPC_BASE}/{GRPC_MATCH_METHOD}"

        data = self._stream_grpc(url, payload)
        if len(data) < 6:
            logger.info(f"Betclic gRPC {sport_code}: empty response")
            return []

        rows = []
        match_ids = []  # (match_id, match_name, league_code) for detail calls

        for frame_payload in self._parse_grpc_frames(data):
            top = _pb_decode_message(frame_payload, max_depth=8)
            wrapper = _pb_get(top, 1, "M")
            if not wrapper:
                continue

            seen_detail_ids = set()
            for match_fields in _pb_get_all(wrapper, 3, "M"):
                match_rows, mid, mname, comp_name, starts_at = self._parse_match(
                    match_fields, sport_code, internal_code
                )
                rows.extend(match_rows)

                if mid is not None and mid not in seen_detail_ids:
                    seen_detail_ids.add(mid)
                    match_ids.append(
                        {
                            "mid": mid,
                            "title": mname or "",
                            "competition": comp_name or "",
                            "starts_at": starts_at or "",
                        }
                    )

        # --- Tier 2: Match details (totals, spreads) in parallel ---
        if match_ids:
            detail_rows = self._fetch_match_details(match_ids, sport_code, internal_code)
            rows.extend(detail_rows)

        return rows

    def fetch_all_sports_parallel(
        self, sport_codes: Optional[List[str]] = None
    ) -> List[dict]:
        """Fetch all configured sports concurrently (one pool for listing + details per sport)."""
        codes = sport_codes if sport_codes is not None else list(BETCLIC_SPORTS.keys())
        if not codes:
            return []
        all_rows: List[dict] = []

        def _fetch_one(sport_code: str) -> List[dict]:
            internal = BETCLIC_SPORTS.get(sport_code, "UNKNOWN")
            try:
                rows = self._fetch_sport_grpc(sport_code)
            except Exception as e:
                logger.error("Betclic %s: %s", sport_code, e)
                return []
            by_prefix = Counter((r["market_type"] or "").split("_", 1)[0] for r in rows)
            logger.info(
                "Betclic %s: %s odds | by_prefix=%s",
                internal,
                len(rows),
                dict(by_prefix),
            )
            return rows

        workers = max(1, len(codes))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_fetch_one, sc): sc for sc in codes}
            for fut in as_completed(futures):
                all_rows.extend(fut.result())

        return all_rows

    def _fetch_match_details(
        self, match_ids: List[dict], sport_code: str, internal_code: str
    ) -> List[dict]:
        """Fetch detailed markets for multiple matches in parallel."""
        detail_url = f"{GRPC_BASE}/offering.access.api.MatchService/GetMatchWithNotification"
        rows = []
        nw = _grpc_detail_max_workers()

        def _fetch_one(meta: dict) -> List[dict]:
            payload = _pb_encode_varint_field(1, meta["mid"]) + _pb_encode_string(2, "fr")
            data = self._stream_grpc(detail_url, payload, read_timeout=5)
            if len(data) < 10:
                return []
            return self._parse_match_detail(data, sport_code, internal_code, meta)

        with ThreadPoolExecutor(max_workers=min(nw, max(1, len(match_ids)))) as pool:
            futures = {
                pool.submit(_fetch_one, meta): meta.get("title", "")
                for meta in match_ids
            }
            for f in as_completed(futures):
                try:
                    rows.extend(f.result())
                except Exception:
                    pass

        return rows

    def _parse_match_detail(
        self, data: bytes, sport_code: str, internal_code: str, meta: dict
    ) -> List[dict]:
        """Parse match detail response for total/spread markets.

        Match detail structure:
          top → f1 (wrapper) → f2 (repeated market combos)
          Each f2 has f9 (repeated markets) with:
            f1: selection (f10=name, f12=odds)
            f3: market name ("Nombre total de buts", etc.)
        """
        match_name = meta.get("title") or ""
        competition = meta.get("competition") or ""
        starts_at = meta.get("starts_at") or ""
        mid = meta.get("mid")

        rows = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for frame_payload in self._parse_grpc_frames(data):
            top = _pb_decode_message(frame_payload, max_depth=8)
            outer = _pb_get(top, 1, "M")
            if not outer:
                continue

            # Teams from outer.f4 (contestants)
            contestants = _pb_get_all(outer, 4, "M")
            home_team = ""
            away_team = ""
            if len(contestants) >= 2:
                home_team = _pb_get(contestants[0], 3, "S") or ""
                away_team = _pb_get(contestants[1], 3, "S") or ""

            # Use match name from outer if available
            detail_name = _pb_get(outer, 8, "S") or match_name

            # Collect unique odds (recursive walk: player props may not be under f2→f9 only)
            seen_keys = set()

            for mkt_name, sel_msgs in _iter_market_blocks(outer):
                for sel_msg in sel_msgs:
                    sel_name = _pb_string_field(sel_msg, 10) or _pb_string_field(sel_msg, 11) or ""
                    odds_val = _pb_get(sel_msg, 12, "D")
                    if not sel_name or not odds_val or odds_val <= 1.0:
                        continue

                    odds = round(odds_val, 2)
                    market_type, selection = self._classify_detail_market(
                        sport_code, mkt_name, sel_name, home_team, away_team
                    )
                    if not market_type:
                        market_type = _spec_raw_key(sport_code)
                        selection = f"{mkt_name.strip()}||{sel_name.strip()}"

                    key = (detail_name, market_type, selection)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    rows.append({
                        "bookmaker": "Betclic",
                        "title": detail_name,
                        "market_type": market_type,
                        "selection": selection,
                        "odds": odds,
                        "scraped_at": now_iso,
                        "is_live": False,
                        "competition": competition,
                        "starts_at": starts_at,
                        "match_id": mid,
                        "market_label": mkt_name.strip(),
                        "sport_code": sport_code,
                    })

        return rows

    def _classify_detail_market(
        self,
        sport_code: str,
        mkt_name: str,
        sel_name: str,
        home: str,
        away: str,
    ) -> Tuple[Optional[str], str]:
        """Map Betclic detail markets to spec-aligned (market_type, selection).

        Only markets under scraping_specifications_clean §3.1 (prematch). Other lines → spec_raw.
        """
        m = _norm_key(mkt_name)
        sel_raw = sel_name.strip()
        hn = _norm_key(home)
        an = _norm_key(away)
        ou = _parse_over_under_line(sel_name)
        yn = _parse_yes_no(sel_name)

        def _team_from_sel() -> Optional[str]:
            sn = _norm_key(sel_name)
            if hn and (sn == hn or sel_raw == home):
                return home
            if an and (sn == an or sel_raw == away):
                return away
            return None

        # ----- Football (spec §3.1) -----
        if sport_code == "football":
            fp = _foot_period(mkt_name)
            if fp is None:
                if "mi-temps" in m or "1ere mi" in m or "1ère mi" in m:
                    fp = "ht"
                else:
                    fp = "ft"

            # Both teams to score
            if any(
                k in m
                for k in (
                    "deux equipes marquent",
                    "les deux equipes marquent",
                    "both teams to score",
                    "btts",
                    "les deux marquent",
                    "equipes marquent",
                    "bbm",
                )
            ):
                tag = f"foot_btts_{fp}"
                if yn:
                    return tag, yn
                return None, ""

            # Double chance
            if "double chance" in m or "chance double" in m:
                tag = f"foot_dc_{fp}"
                return tag, sel_raw

            # First half – first goal (Team A / Neither / Team B)
            if (
                any(
                    k in m
                    for k in (
                        "premier but",
                        "première équipe à marquer sur la mi-temps",
                        "first goal",
                    )
                )
                or ("qui marque" in m and ("mi-temps" in m or "1ere mi" in m))
            ) and ("mi-temps" in m or "1ere mi" in m or "1st half" in m or fp == "ht"):
                sn = _norm_key(sel_name)
                if "aucun" in sn or "neither" in sn or "no goal" in sn:
                    return "foot_1h_first_goal", "Neither"
                t = _team_from_sel()
                if t:
                    return "foot_1h_first_goal", t
                return "foot_1h_first_goal", sel_raw

            # First team to score (full match)
            if any(
                k in m
                for k in (
                    "premiere equipe a marquer",
                    "première équipe à marquer",
                    "qui marque en premier",
                    "first team to score",
                    "equipe qui ouvre le score",
                )
            ):
                t = _team_from_sel()
                if t:
                    return "foot_first_score", t
                if "aucun" in _norm_key(sel_name) or "no goal" in _norm_key(sel_name):
                    return "foot_first_score", "Neither"
                return "foot_first_score", sel_raw

            # First half result without draw
            if any(
                k in m
                for k in (
                    "sans match nul",
                    "pas de nul",
                    "no draw",
                    "draw no bet",
                    "rembourse si match nul",
                    "rembourse si nul",
                    "vainqueur sans nul",
                    "sans nul",
                )
            ) and ("mi-temps" in m or "1ere mi" in m or "1st half" in m):
                t = _team_from_sel()
                if t:
                    return "foot_1h_no_draw", t
                return "foot_1h_no_draw", sel_raw

            # Win without conceding / to nil
            if any(
                k in m
                for k in (
                    "sans encaisser",
                    "victoire sans encaisser",
                    "win to nil",
                    "to nil",
                    "victoire a zero",
                    "a zero",
                    "clean sheet",
                    "blanchissage",
                )
            ):
                if hn and (_team_name_in_market(hn, m) or "domicile" in m or "equipe 1" in m):
                    tag = f"foot_win_nil_home_{fp}"
                elif an and (_team_name_in_market(an, m) or "exterieur" in m or "equipe 2" in m):
                    tag = f"foot_win_nil_away_{fp}"
                else:
                    t = _team_from_sel()
                    if t == home:
                        tag = f"foot_win_nil_home_{fp}"
                    elif t == away:
                        tag = f"foot_win_nil_away_{fp}"
                    else:
                        tag = f"foot_win_nil_home_{fp}"
                if yn:
                    return tag, yn
                return tag, sel_raw

            # Goal handicaps (Asian / European) — before "team to score" so HDC labels are not swallowed
            if (
                (
                    "handicap" in m
                    or "marge" in m
                    or "ecart" in m
                    or "hdc" in m
                    or "asian" in m
                    or "europeen" in m
                    or ("but" in m and ("ecart" in m or "marge" in m))
                )
                and "corner" not in m
                and "carton" not in m
            ):
                if hn and (_team_name_in_market(hn, m) or "domicile" in m or "equipe 1" in m):
                    tag = f"foot_hcp_team_a_{fp}"
                elif an and (_team_name_in_market(an, m) or "exterieur" in m or "equipe 2" in m):
                    tag = f"foot_hcp_team_b_{fp}"
                else:
                    t = _team_from_sel()
                    if t == home:
                        tag = f"foot_hcp_team_a_{fp}"
                    elif t == away:
                        tag = f"foot_hcp_team_b_{fp}"
                    else:
                        sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                        if sm:
                            tag = f"foot_hcp_team_a_{fp}"
                            return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                        # Asian / « Écart de buts » : libellés longs sans (+/-) sur la sélection
                        sn = _norm_key(sel_raw)
                        if hn and _team_name_in_market(hn, sn):
                            tag = f"foot_hcp_team_a_{fp}"
                        elif an and _team_name_in_market(an, sn):
                            tag = f"foot_hcp_team_b_{fp}"
                        else:
                            tag = f"foot_hcp_line_{fp}"
                sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                if sm:
                    return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                return tag, sel_raw

            # Team to score yes/no
            if (
                "marque" in m
                and "handicap" not in m
                and (
                    yn is not None
                    or "oui" in m
                    or "yes" in m
                    or "non" in m
                    or "domicile" in m
                    or "exterieur" in m
                    or "equipe 1" in m
                    or "equipe 2" in m
                    or "equipe a" in m
                    or "equipe b" in m
                )
            ):
                is_home = hn and (_team_name_in_market(hn, m) or "domicile" in m or "equipe 1" in m)
                is_away = an and (_team_name_in_market(an, m) or "exterieur" in m or "equipe 2" in m)
                if not is_home and not is_away:
                    is_home = _team_from_sel() == home
                    is_away = _team_from_sel() == away
                if is_home:
                    tag = f"foot_team_a_score_{fp}"
                elif is_away:
                    tag = f"foot_team_b_score_{fp}"
                else:
                    tag = f"foot_team_a_score_{fp}"
                if yn:
                    return tag, yn
                return tag, sel_raw

            # Team total goals
            if "total" in m and "but" in m:
                if hn and (_team_name_in_market(hn, m) or "domicile" in m or "equipe 1" in m):
                    tag = f"foot_total_team_a_{fp}"
                elif an and (_team_name_in_market(an, m) or "exterieur" in m or "equipe 2" in m):
                    tag = f"foot_total_team_b_{fp}"
                elif "equipe a" in m or "team a" in m:
                    tag = f"foot_total_team_a_{fp}"
                elif "equipe b" in m or "team b" in m:
                    tag = f"foot_total_team_b_{fp}"
                else:
                    tag = None
                if tag and ou:
                    return tag, f"{ou[0]} {ou[1]}"

            # Match total goals
            if any(
                k in m
                for k in (
                    "nombre total de buts",
                    "total de buts",
                    "total goals",
                    "buts (plus",
                    "plus / moins de buts",
                    "over under buts",
                )
            ) and "equipe" not in m and "domicile" not in m and "exterieur" not in m:
                tag = f"foot_total_goals_{fp}"
                if ou:
                    return tag, f"{ou[0]} {ou[1]}"

            # Moneyline 1X2
            if any(
                k in m
                for k in (
                    "resultat",
                    "1x2",
                    "vainqueur",
                    "qui gagne",
                    "winner",
                    "issue du match",
                    "pari sur le vainqueur",
                    "gagnant du match",
                )
            ):
                tag = f"foot_ml_{fp}"
                if _norm_key(sel_name) in DRAW_LABELS:
                    return tag, "Draw"
                t = _team_from_sel()
                if t:
                    return tag, t
                return tag, sel_raw

        # ----- Tennis -----
        if sport_code == "tennis":
            ts = _tennis_scope(mkt_name)
            if ts is None:
                ts = "ft"
            # Betclic: « Écart de jeux » / « Écart de sets » (souvent champ 10), pas seulement « handicap »
            hc_kw = (
                "handicap" in m
                or "ecart" in m
                or "marge" in m
                or "hdc" in m
                or "asian" in m
            )
            ecart_jeux = (
                "ecart de jeux" in m
                or "ecart des jeux" in m
                or "ecart sur les jeux" in m
            )
            ecart_sets = (
                "ecart de sets" in m
                or "ecart des sets" in m
                or "ecart sur les sets" in m
            )
            # Jeux d'abord si le libellé le dit (évite « manche » + jeux → faux sets)
            if hc_kw and (ecart_jeux or (not ecart_sets and ("jeu" in m or "jeux" in m))):
                tag = f"tennis_games_hcp_{ts}"
                sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                if sm:
                    return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                return tag, sel_raw
            if hc_kw and (ecart_sets or ("set" in m and "jeu" not in m and "jeux" not in m)):
                tag = "tennis_sets_hcp_ft"
                sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                if sm:
                    return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                return tag, sel_raw
            if ("jeu" in m or "jeux" in m) and ("total" in m or "nombre" in m):
                if "joueur 1" in m or "j1" in m or (hn and hn[: min(6, len(hn))] in m):
                    tag = f"tennis_total_games_player_a_{ts}"
                elif "joueur 2" in m or "j2" in m or (an and an[: min(6, len(an))] in m):
                    tag = f"tennis_total_games_player_b_{ts}"
                else:
                    tag = f"tennis_total_games_player_a_{ts}"
                if ou:
                    return tag, f"{ou[0]} {ou[1]}"
                return None, ""
            if any(
                k in m
                for k in (
                    "vainqueur",
                    "gagnant",
                    "winner",
                    "money line",
                    "resultat",
                    "pari sur le vainqueur",
                )
            ):
                tag = f"tennis_ml_{ts}"
                t = _team_from_sel()
                if t:
                    return tag, t
                return tag, sel_raw

        # ----- Basketball -----
        if sport_code == "basketball":
            bp = _basket_period(mkt_name)
            if "handicap" in m or "ecart" in m or "marge" in m or "spread" in m:
                tag = f"basket_spread_{bp}"
                sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                if sm:
                    return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                return tag, sel_raw
            # NBA player performance (spec) — before game totals (same FR labels as "Total points")
            if _is_nba_player_prop_market(mkt_name, sel_raw) and (
                re.search(
                    r"\b(points|rebonds|passes|assists|rebounds|pts)\b", m, re.I
                )
                or "3" in m
                or "triple" in m
                or "double" in m
            ):
                if "triple-double" in m or "triple double" in m:
                    return "prop_nba_triple_double", yn or sel_raw
                if "double-double" in m or "double double" in m:
                    return "prop_nba_double_double", yn or sel_raw
                if "3" in m and ("point" in m or "pts" in m or "panier" in m):
                    if ou:
                        return "prop_nba_3pm", f"{ou[0]} {ou[1]}"
                if "points + assists + rebonds" in m or "pra" in m:
                    if ou:
                        return "prop_nba_par", f"{ou[0]} {ou[1]}"
                if "rebond" in m or "rebound" in m:
                    if ou:
                        return "prop_nba_rebounds", f"{ou[0]} {ou[1]}"
                if "passe" in m or "assist" in m:
                    if ou:
                        return "prop_nba_assists", f"{ou[0]} {ou[1]}"
                if "point" in m or "pts" in m:
                    if ou:
                        return "prop_nba_points", f"{ou[0]} {ou[1]}"
                if ou:
                    return "prop_nba_points", f"{ou[0]} {ou[1]}"
                return "prop_nba_points", sel_raw
            if "total" in m and ("point" in m or "pts" in m or "points" in m):
                tag = f"basket_total_{bp}"
                if ou:
                    return tag, f"{ou[0]} {ou[1]}"
                return None, ""
            if any(k in m for k in ("vainqueur", "gagnant", "winner", "money line", "resultat")):
                tag = f"basket_ml_{bp}"
                t = _team_from_sel()
                if t:
                    return tag, t
                return tag, sel_raw

        # ----- Ice hockey -----
        if sport_code == "ice-hockey":
            hp = _hockey_period(mkt_name)
            if hp is None:
                hp = "rt"
            if "handicap" in m or "ecart" in m:
                if hn and (_team_name_in_market(hn, m) or "domicile" in m or "equipe 1" in m):
                    tag = f"hockey_hcp_team_a_{hp}"
                elif an and (_team_name_in_market(an, m) or "exterieur" in m):
                    tag = f"hockey_hcp_team_b_{hp}"
                else:
                    tag = f"hockey_hcp_team_a_{hp}"
                sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
                if sm:
                    return tag, f"{sm.group(1).strip()} {sm.group(2)}"
                return tag, sel_raw
            if "total" in m and "but" in m:
                if hn and (_team_name_in_market(hn, m) or "domicile" in m):
                    tag = f"hockey_total_team_a_{hp}"
                elif an and (_team_name_in_market(an, m) or "exterieur" in m):
                    tag = f"hockey_total_team_b_{hp}"
                elif "equipe a" in m:
                    tag = f"hockey_total_team_a_{hp}"
                elif "equipe b" in m:
                    tag = f"hockey_total_team_b_{hp}"
                else:
                    tag = f"hockey_total_{hp}"
                if ou:
                    return tag, f"{ou[0]} {ou[1]}"
                return None, ""
            if any(k in m for k in ("vainqueur", "gagnant", "money line", "1x2")):
                tag = f"hockey_ml_{hp}"
                t = _team_from_sel()
                if t:
                    return tag, t
                return tag, sel_raw
            # Player props (spec: named player — avoid matching team/game markets)
            if "joueur" in m or "player" in m:
                if "tir" in m or "shot" in m:
                    if ou:
                        return "prop_hockey_sog", f"{ou[0]} {ou[1]}"
                if "assist" in m or "passe" in m:
                    if ou:
                        return "prop_hockey_assists", f"{ou[0]} {ou[1]}"
                if "but" in m or "goal" in m:
                    if ou:
                        return "prop_hockey_goals", f"{ou[0]} {ou[1]}"
                if "point" in m:
                    if ou:
                        return "prop_hockey_points", f"{ou[0]} {ou[1]}"
                if ou:
                    return "prop_hockey_points", f"{ou[0]} {ou[1]}"

        # ----- Fallback: legacy totals / spreads -----
        if ou:
            return "total", f"{ou[0]} {ou[1]}"
        sm = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_raw)
        if sm:
            return "spread", f"{sm.group(1).strip()} {sm.group(2)}"
        return None, ""

    def _parse_frame(self, payload: bytes, sport_code: str, internal_code: str) -> List[dict]:
        """Parse a single gRPC frame (protobuf message) into odds rows."""
        top = _pb_decode_message(payload, max_depth=8)
        rows = []

        # Response structure: top → field 1 (wrapper) → field 3 (repeated matches)
        wrapper = _pb_get(top, 1, "M")
        if not wrapper:
            return rows

        matches = _pb_get_all(wrapper, 3, "M")
        for match_fields in matches:
            match_rows, _, _, _, _ = self._parse_match(match_fields, sport_code, internal_code)
            rows.extend(match_rows)

        return rows

    def _parse_match(
        self, fields: list, sport_code: str, internal_code: str
    ) -> Tuple[List[dict], Optional[int], str, str, str]:
        """Parse a single match protobuf message into odds rows.

        Match protobuf structure (field 3 of top-level):
          f1: match ID (varint)
          f2: match name "Home - Away" (string)
          f3: start time ISO8601 (string)
          f8: competition info (message)
            f1: competition ID (varint)
            f2: competition name (string or bytes)
            f5: country code (string)
          f9: market/market group (message)
            f16: selections (repeated message)
              f10: selection name (string)
              f12: odds (double)
          f12: contestants (repeated message)
            f3: full team name (string)
            f4: short team name (string)
        """
        mid = _pb_get(fields, 1, "V")
        match_name = _pb_get(fields, 2, "S")
        starts_at = _pb_string_field(fields, 3)
        if not match_name:
            return [], mid, "", "", ""

        # Competition info (f8 contains competition ID, name, country)
        # f8.f2 may be string or bytes (containing UTF-8 with non-ASCII chars)
        # f8.f3.f2 has the human-readable sport name
        comp_msg = _pb_get(fields, 8, "M")
        comp_name = ""
        if comp_msg:
            # Try string first
            comp_name = _pb_get(comp_msg, 2, "S") or ""
            if not comp_name:
                # Try bytes → decode as UTF-8
                comp_bytes = _pb_get(comp_msg, 2, "B")
                if comp_bytes and isinstance(comp_bytes, bytes):
                    try:
                        comp_name = comp_bytes.decode("utf-8", errors="replace")
                    except Exception:
                        pass

        # Map competition to internal league code
        league_code = map_league(comp_name, internal_code, source="betclic")
        if league_code is None:
            return [], mid, match_name, comp_name, starts_at

        # Extract team names from contestants (field 12)
        contestants = _pb_get_all(fields, 12, "M")
        home_team = ""
        away_team = ""
        if len(contestants) >= 2:
            home_team = _pb_get(contestants[0], 3, "S") or _pb_get(contestants[0], 4, "S") or ""
            away_team = _pb_get(contestants[1], 3, "S") or _pb_get(contestants[1], 4, "S") or ""
        elif " - " in match_name:
            parts = match_name.split(" - ", 1)
            home_team = parts[0].strip()
            away_team = parts[1].strip()

        # Extract market data (field 9)
        market_msg = _pb_get(fields, 9, "M")
        if not market_msg:
            return [], mid, match_name, comp_name, starts_at

        # Selections are in repeated field 16 of the market message
        selections = _pb_get_all(market_msg, 16, "M")
        if not selections:
            return [], mid, match_name, comp_name, starts_at

        now_iso = datetime.now(timezone.utc).isoformat()
        rows = []

        for sel_fields in selections:
            sel_name = _pb_string_field(sel_fields, 10) or _pb_string_field(sel_fields, 11) or ""
            odds_val = _pb_get(sel_fields, 12, "D")
            if not sel_name or odds_val is None or odds_val <= 1.0:
                continue

            odds = round(odds_val, 2)

            # Classify market type and build selection
            market_type, selection = self._classify_selection(
                sel_name, home_team, away_team, sport_code
            )
            if not market_type:
                continue

            rows.append({
                "bookmaker": "Betclic",
                "title": match_name,
                "market_type": market_type,
                "selection": selection,
                "odds": odds,
                "scraped_at": now_iso,
                "is_live": False,
                "competition": comp_name,
                "starts_at": starts_at,
                "match_id": mid,
                "market_label": "",
                "sport_code": sport_code,
            })

        return rows, mid, match_name, comp_name, starts_at

    def _classify_selection(
        self, sel_name: str, home: str, away: str, sport_code: str
    ) -> Tuple[Optional[str], str]:
        """Classify listing (tier-1) selections into spec-aligned market_type."""
        sel_lower = sel_name.lower().strip()

        def _ml_tag() -> str:
            if sport_code == "football":
                return "foot_ml_ft"
            if sport_code == "tennis":
                return "tennis_ml_ft"
            if sport_code == "basketball":
                return "basket_ml_ft"
            if sport_code == "ice-hockey":
                return "hockey_ml_rt"
            return "moneyline"

        # Draw detection
        if sel_lower in DRAW_LABELS:
            tag = _ml_tag()
            if sport_code == "football":
                return tag, "Draw"
            return tag, "Nul"

        # Moneyline: selection is a team name
        if sel_name == home or sel_lower == home.lower():
            return _ml_tag(), home
        if sel_name == away or sel_lower == away.lower():
            return _ml_tag(), away

        # Over/Under (total)
        over_match = re.match(r"(?:plus\s+de|over)\s+([\d.]+)", sel_lower)
        if over_match:
            line = over_match.group(1)
            if sport_code == "football":
                return "foot_total_goals_ft", f"Over {line}"
            if sport_code == "tennis":
                return "tennis_total_games_player_a_ft", f"Over {line}"
            if sport_code == "basketball":
                return "basket_total_ft", f"Over {line}"
            if sport_code == "ice-hockey":
                return "hockey_total_rt", f"Over {line}"
            return "total", f"Over {line}"
        under_match = re.match(r"(?:moins\s+de|under)\s+([\d.]+)", sel_lower)
        if under_match:
            line = under_match.group(1)
            if sport_code == "football":
                return "foot_total_goals_ft", f"Under {line}"
            if sport_code == "tennis":
                return "tennis_total_games_player_a_ft", f"Under {line}"
            if sport_code == "basketball":
                return "basket_total_ft", f"Under {line}"
            if sport_code == "ice-hockey":
                return "hockey_total_rt", f"Under {line}"
            return "total", f"Under {line}"

        # Spread/Handicap
        spread_match = re.match(r"(.+?)\s*\(?([+-]\d+\.?\d*)\)?$", sel_name)
        if spread_match:
            team = spread_match.group(1).strip()
            line = spread_match.group(2)
            sel = f"{team} {line}"
            if sport_code == "football":
                return "foot_hcp_team_a_ft", sel
            if sport_code == "tennis":
                return "tennis_games_hcp_ft", sel
            if sport_code == "basketball":
                return "basket_spread_ft", sel
            if sport_code == "ice-hockey":
                return "hockey_hcp_team_a_rt", sel
            return "spread", sel

        # Fallback: partial team name match → moneyline
        if home and sel_name.startswith(home[:5]):
            return _ml_tag(), home
        if away and sel_name.startswith(away[:5]):
            return _ml_tag(), away

        return None, ""

    def scrape(self, supabase_client, sports: Optional[List[str]] = None) -> int:
        """Scrape all sports and save to bookmaker_odds.

        Args:
            supabase_client: SupabaseClient with upsert_bookmaker_odds()
            sports: Optional list of gRPC sport codes to scrape.
                    Defaults to all BETCLIC_SPORTS.
        """
        sport_list = sports or list(BETCLIC_SPORTS.keys())
        reset_unmapped_tournaments()

        t0 = time.time()
        logger.info("Fetching Betclic odds (gRPC-Web)...")

        all_rows = self.fetch_all_sports_parallel(sport_list)

        if not all_rows:
            logger.info(f"Betclic total: 0 odds in {time.time() - t0:.1f}s")
            return 0

        # Log unmapped tournaments
        unmapped = get_unmapped_tournaments()
        if unmapped:
            logger.warning(f"Betclic UNMAPPED: {', '.join(sorted(unmapped)[:10])}")

        count = supabase_client.upsert_bookmaker_odds(all_rows)
        elapsed = time.time() - t0
        logger.info(f"Betclic total: {count} odds in {elapsed:.1f}s")
        return count


# ======================================================================
# Standalone entry point
# ======================================================================

def run_cycle():
    """Run one scrape cycle (standalone)."""
    from valuebot.config import Settings
    from valuebot.db.client import SupabaseClient

    settings = Settings.from_env()
    db = SupabaseClient(
        settings.supabase_url, settings.supabase_key, redis_url=settings.redis_url
    )

    scraper = BetclicScraper()
    return scraper.scrape(db)


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    if "--debug" in sys.argv:
        # Debug mode: fetch and dump raw protobuf structure
        sport = sys.argv[sys.argv.index("--debug") + 1] if len(sys.argv) > sys.argv.index("--debug") + 1 else "football"
        scraper = BetclicScraper()
        rows = scraper._fetch_sport_grpc(sport, limit=5)
        for r in rows:
            print(f"  {r['market_type']:>10} | {r.get('title','')[:40]:40} | {r['selection']:20} | {r['odds']}")
    elif "--supabase" in sys.argv:
        try:
            count = run_cycle()
            print(f"Scraped {count} Betclic odds")
        except ImportError as e:
            logging.error(
                "Cycle Supabase/valuebot indisponible (%s). "
                "Sans valuebot, lancez: python betclic.py",
                e,
            )
            raise SystemExit(1) from e
    elif "--check-json" in sys.argv:
        idx = sys.argv.index("--check-json")
        jpath = Path(sys.argv[idx + 1]) if len(sys.argv) > idx + 1 else Path("output.json")
        data = json.loads(jpath.read_text(encoding="utf-8"))
        dup = count_duplicate_lines_in_output_json(data)
        print(f"{jpath}: doublons extra markets={dup['markets']} props={dup['props']}")
        raise SystemExit(1 if (dup["markets"] or dup["props"]) else 0)
    else:
        _root = Path(__file__).resolve().parent

        scraper = BetclicScraper()
        all_rows = scraper.fetch_all_sports_parallel()

        payload = rows_to_output_structure(all_rows)
        out_path = _root / "output.json"
        out_path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        total = sum(
            s.get("total_rows", 0) for s in payload.get("sports", {}).values()
        )
        dup = count_duplicate_lines_in_output_json(payload)
        if dup["markets"] or dup["props"]:
            logger.warning("Doublons détectés dans output (anormal): %s", dup)
        print(f"Wrote {out_path} ({total} lignes marchés+props, doublons: {dup})")
