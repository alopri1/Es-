"""
INDEX ICT + AMD Engine v1  (futures NQ / ES)
Fondamental pondere -> Biais combine (fonda + structure HTF) -> Niveaux (liquidite, VWAP, Volume Profile)
-> POI (Order Block, FVG, OTE) -> Sweep + MSS -> Killzone -> Intermarket (VIX, US10Y, DXY) + SMT NQ/ES
-> AMD : A (accumulation, range + POC de A) -> M (sweep + MSS) -> D (distribution vers le POC de A)
-> SETUP CONFIRMED -> Entry / SL / TP / R:R

Outil d'aide a la decision, pas un conseil financier. A tester en demo avant tout argent reel.

Render : Build  = pip install -r requirements.txt
         Start  = gunicorn app:app --workers 1 --threads 4 --timeout 120
Variables d'environnement (toutes optionnelles) :
  FRED_KEY = cle FRED gratuite (https://fred.stlouisfed.org/docs/api/api_key.html) -> fondamental complet
  INDEX    = NQ (defaut) ou ES
  AMD_A    = fenetre de l'accumulation en heure de New York, "20-0" par defaut (20:00 -> 00:00)
Tout le reste (Yahoo Finance, CFTC, calendrier) est gratuit et sans cle.
"""
import os
import math
import time
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, Response, request

FRED_KEY = os.getenv("FRED_KEY")
DEFAULT_SYM = os.getenv("INDEX", "NQ").upper()
AMD_A = os.getenv("AMD_A", "20-0")
MIN_RR = 2.0             # R:R minimum (ICT)
AMD_MIN_RR = 1.5         # R:R minimum vers le POC de A (AMD)
BIAS_TH = 20             # seuil de biais combine (-100..+100)
MIN_SCORE = 12           # score de confluence ICT minimum (sur 23)
AMD_MIN_SCORE = 9        # score AMD minimum (sur 16)
UA = {"User-Agent": "Mozilla/5.0"}
NY = ZoneInfo("America/New_York")

IDX = {
    "NQ": dict(y="NQ=F", name="Nasdaq 100 (NQ)", other="ES", cot="209742", tick=0.25, rate_w=1.25),
    "ES": dict(y="ES=F", name="S&P 500 (ES)", other="NQ", cot="13874A", tick=0.25, rate_w=1.0),
}
if DEFAULT_SYM not in IDX:
    DEFAULT_SYM = "NQ"

app = Flask(__name__)
LOCK = threading.Lock()
CACHE = {}
STATE = {}

# nom: (range Yahoo, intervalle, ttl cache s, nb max de bougies)
TFS = {
    "1m": ("1d", "1m", 55, 500), "5m": ("5d", "5m", 110, 1200), "15m": ("1mo", "15m", 200, 800),
    "1h": ("6mo", "1h", 600, 800), "1d": ("2y", "1d", 1800, 400),
    "1w": ("10y", "1wk", 3600, 300), "1M": ("10y", "1mo", 7200, 130),
}
TFW = {"1m": 0.5, "5m": 1, "15m": 1, "1h": 2, "4h": 3, "1d": 3, "1w": 3, "1M": 3}
ORDER = ["1M", "1w", "1d", "4h", "1h", "15m", "5m", "1m"]
LOOK = {"1M": 24, "1w": 26, "1d": 20, "4h": 40, "1h": 48, "15m": 64, "5m": 72, "1m": 60}
TGT_REF = {"VAH", "VAL", "POC", "VAH J-1", "VAL J-1", "POC J-1", "VWAP+1s", "VWAP-1s", "VWAP+2s", "VWAP-2s",
           "Open jour", "Open semaine", "POC A", "VAH A", "VAL A"}


# ====================== OUTILS GENERAUX ======================

def now_ts():
    return time.time()


def ny_now():
    return datetime.fromtimestamp(now_ts(), NY)


def last_anchor(hour, now=None):
    """Dernier hh:00 (heure de New York) <= maintenant."""
    n = now or ny_now()
    a = n.replace(hour=hour, minute=0, second=0, microsecond=0)
    if a > n:
        a -= timedelta(days=1)
    return a


def state(sym):
    return STATE.setdefault(sym, {"plan": None, "log": []})


def log(sym, msg):
    L = state(sym)["log"]
    L.insert(0, {"t": datetime.fromtimestamp(now_ts(), timezone.utc).strftime("%d/%m %H:%M UTC"), "msg": msg})
    del L[15:]


def cached(key, ttl, fn):
    e = CACHE.get(key)
    if e and now_ts() - e[0] < ttl:
        return e[1]
    try:
        v = fn()
    except Exception:
        if e:
            return e[1]
        raise
    CACHE[key] = (now_ts(), v)
    return v


def clamp(x):
    return max(-1.0, min(1.0, x))


def sign(x, th):
    return 1 if x >= th else (-1 if x <= -th else 0)


# ====================== DONNEES (gratuites) ======================

def fred(series, n=15):
    if not FRED_KEY:
        raise RuntimeError("FRED_KEY manquante")
    r = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params=dict(series_id=series, api_key=FRED_KEY, file_type="json",
                    sort_order="desc", limit=n), timeout=20).json()
    return [float(o["value"]) for o in r["observations"] if o["value"] != "."]


def yahoo(symbol, rng, interval):
    last = None
    for host in ("query1", "query2"):
        try:
            r = requests.get(
                "https://%s.finance.yahoo.com/v8/finance/chart/%s" % (host, symbol),
                params=dict(range=rng, interval=interval), headers=UA, timeout=20).json()
            res = r["chart"]["result"][0]
            q = res["indicators"]["quote"][0]
            out = []
            for i in range(len(res["timestamp"])):
                if None in (q["open"][i], q["high"][i], q["low"][i], q["close"][i]):
                    continue
                out.append(dict(t=res["timestamp"][i], o=q["open"][i], h=q["high"][i],
                                l=q["low"][i], c=q["close"][i], v=(q["volume"][i] or 0)))
            if out:
                return out
        except Exception as e:
            last = e
    raise last or RuntimeError("Yahoo indisponible")


def get_tf(sym, name):
    rng, iv, ttl, mx = TFS[name]
    return cached("tf" + sym + name, ttl, lambda: yahoo(IDX[sym]["y"], rng, iv)[-mx:])


def safe_tf(sym, name):
    try:
        return get_tf(sym, name)
    except Exception:
        return None


def closes(symbol, rng="3mo"):
    return [x["c"] for x in cached("cl" + symbol + rng, 1800, lambda: yahoo(symbol, rng, "1d"))]


def resample(c, sec, anchor=0):
    out, cur = [], None
    for x in c:
        b = (x["t"] - anchor) // sec * sec + anchor
        if cur and cur["t"] == b:
            cur["h"] = max(cur["h"], x["h"])
            cur["l"] = min(cur["l"], x["l"])
            cur["c"] = x["c"]
            cur["v"] += x["v"]
        else:
            cur = dict(x)
            cur["t"] = b
            out.append(cur)
    return out


def anchor4h():
    """Les bougies 4H des futures sont alignees sur 18:00 New York."""
    off = int(ny_now().utcoffset().total_seconds())
    return (18 * 3600 - off) % 14400


def flip(c, d):
    """d=-1 : on inverse les prix pour reutiliser toute la logique 'achat' cote vente."""
    if d == 1:
        return c
    return [dict(t=x["t"], o=-x["o"], h=-x["l"], l=-x["h"], c=-x["c"], v=x["v"]) for x in c]


# ====================== 1. FONDAMENTAL (pondere, -100..+100) ======================

def fundamentals(sym, d1):
    S = IDX[sym]
    items = []

    def add(label, x, w):
        items.append({"label": label, "score": round(clamp(x), 2), "w": w})

    def run(fn):
        try:
            fn()
        except Exception:
            pass

    def vix():
        v = closes("^VIX")
        ch = v[-1] / v[-22] - 1 if len(v) > 22 else v[-1] / v[0] - 1
        add("VIX %.1f (%+.0f%% / 1 mois) : volatilite basse = actions soutenues" % (v[-1], ch * 100),
            -(v[-1] - 18) / 10 * 0.6 - ch / 0.4 * 0.4, 3)

    def us10():
        t = closes("^TNX")
        k = 0.1 if t[-1] > 20 else 1.0
        ch = (t[-1] - (t[-22] if len(t) > 22 else t[0])) * k
        add("Rendement US 10 ans (1 mois) : %+.2f pt" % ch, -ch / 0.3, 2 * S["rate_w"])

    def us2():
        y = fred("DGS2", 25)
        ch = y[0] - y[-1]
        add("Rendement US 2 ans (1 mois) : %+.2f pt" % ch, -ch / 0.3, 1.5)

    def real():
        y = fred("DFII10", 25)
        ch = y[0] - y[-1]
        add("Rendement reel US 10 ans (1 mois) : %+.2f pt" % ch, -ch / 0.25, 2 * S["rate_w"])

    def curve():
        y = fred("T10Y2Y", 25)
        add("Courbe 10Y-2Y : %.2f pt (%+.2f / 1 mois)" % (y[0], y[0] - y[-1]),
            0.5 * clamp(y[0] / 1.0) + 0.5 * clamp((y[0] - y[-1]) / 0.3), 1)

    def nfci():
        v = fred("NFCI", 8)
        add("Conditions financieres NFCI %.2f (negatif = souple)" % v[0],
            -(v[0] + 0.4) / 0.5 * 0.6 - (v[0] - v[-1]) / 0.1 * 0.4, 2)

    def hy():
        y = fred("BAMLH0A0HYM2", 25)
        add("Spread credit High Yield %.2f (%+.2f / 1 mois)" % (y[0], y[0] - y[-1]),
            -(y[0] - 3.5) / 1.5 * 0.5 - (y[0] - y[-1]) / 0.4 * 0.5, 2.5)

    def walcl():
        w = fred("WALCL", 6)
        ch = w[0] / w[4] - 1
        add("Bilan Fed (4 sem.) : %+.2f%% (liquidite)" % (ch * 100), ch / 0.01, 1)

    def dxy():
        c = closes("DX-Y.NYB")
        dev = c[-1] / (sum(c[-20:]) / 20) - 1
        add("DXY %.2f vs moyenne 20j : %+.2f%% (dollar fort = pression)" % (c[-1], dev * 100), -dev / 0.01, 1)

    def cpi():
        x = fred("CPIAUCSL", 15)
        n, p = x[0] / x[12] - 1, x[1] / x[13] - 1
        d = (n - p) * 100
        add("Inflation US : %.2f%% (tendance %+.2f)" % (n * 100, d), -d / 0.2, 1)

    def claims():
        c = fred("ICSA", 12)
        a4, a12 = sum(c[:4]) / 4, sum(c) / len(c)
        add("Inscriptions chomage US : 4 sem. vs 12 sem. %+.1f%%" % ((a4 / a12 - 1) * 100), -(a4 / a12 - 1) / 0.05, 1)

    def trend():
        c = [x["c"] for x in d1]
        if len(c) < 60:
            return
        m50 = sum(c[-50:]) / 50
        m200 = sum(c[-200:]) / min(200, len(c))
        add("Tendance de fond : %+.1f%% vs MM50, %+.1f%% vs MM200" % ((c[-1] / m50 - 1) * 100, (c[-1] / m200 - 1) * 100),
            0.5 * clamp((c[-1] / m50 - 1) / 0.03) + 0.5 * clamp((c[-1] / m200 - 1) / 0.07), 2.5)

    def breadth():
        a, b = closes("RSP"), closes("SPY")
        rel = (a[-1] / a[-21] - 1) - (b[-1] / b[-21] - 1)
        add("Largeur du marche (RSP vs SPY, 20j) : %+.1f%%" % (rel * 100), rel / 0.02, 1)

    for fn in (vix, us10, us2, real, curve, nfci, hy, walcl, dxy, cpi, claims, trend, breadth):
        run(fn)
    if not items:
        raise RuntimeError("Fondamental indisponible")
    return items


# ====================== 2. ORDER FLOW (proxys gratuits, volume reel CME) ======================

def close_delta(bars):
    tot = sum(x["v"] for x in bars) or 1
    d = sum(x["v"] * ((2 * x["c"] - x["h"] - x["l"]) / (x["h"] - x["l"]) if x["h"] > x["l"] else 0)
            for x in bars)
    return d / tot


def cot_item(sym):
    try:
        rows = requests.get(
            "https://publicreporting.cftc.gov/resource/6dca-aqww.json",
            params={"cftc_contract_market_code": IDX[sym]["cot"],
                    "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 6},
            headers=UA, timeout=20).json()
        net = [int(float(r["noncomm_positions_long_all"])) - int(float(r["noncomm_positions_short_all"]))
               for r in rows]
        oi = float(rows[0].get("open_interest_all") or 0) or 1.0
        ch = net[0] - net[3]
        ratio = ch / oi
        return {"label": "COT speculateurs : net %+d (4 sem. : %+d)" % (net[0], ch),
                "score": 1 if ratio > 0.01 else (-1 if ratio < -0.01 else 0)}
    except Exception:
        return {"label": "COT indisponible", "score": 0}


def delta_items(c5):
    out = []
    sess0 = last_anchor(18).timestamp()
    for label, bars in (("24 h", c5[-288:]), ("session (depuis 18:00 NY)", [x for x in c5 if x["t"] >= sess0]),
                        ("2 h", c5[-24:])):
        if len(bars) < 6:
            continue
        r = close_delta(bars)
        out.append({"label": "Delta estime futures (%s) : %+.1f%%" % (label, r * 100),
                    "score": 1 if r > 0.05 else (-1 if r < -0.05 else 0)})
    return out


# ====================== 3. NEWS ======================

def calendar():
    r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", headers=UA, timeout=20).json()
    now = datetime.fromtimestamp(now_ts(), timezone.utc)
    out = []
    for e in r:
        if e.get("impact") != "High" or e.get("country") != "USD":
            continue
        t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
        m = int((t - now).total_seconds() / 60)
        if -30 <= m <= 24 * 60:
            out.append({"title": e["title"], "country": e["country"],
                        "time": t.strftime("%a %H:%M UTC"), "minutes": m})
    return sorted(out, key=lambda x: x["minutes"])


# ====================== 4. OUTILS PRICE ACTION / ICT ======================

def atr(c, n=14):
    n = min(n, len(c) - 1)
    trs = [max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i - 1]["c"]), abs(c[i]["l"] - c[i - 1]["c"]))
           for i in range(len(c) - n, len(c))]
    return sum(trs) / n


def swings(c, k=3):
    hi, lo = [], []
    for i in range(k, len(c) - k):
        w = c[i - k:i + k + 1]
        if c[i]["h"] == max(x["h"] for x in w):
            hi.append((i, c[i]["h"]))
        if c[i]["l"] == min(x["l"] for x in w):
            lo.append((i, c[i]["l"]))
    return hi, lo


def structure(c, k=3):
    """Tendance (HH/HL, LH/LL) + BOS / CHoCH."""
    hi, lo = swings(c, k)
    if len(hi) < 2 or len(lo) < 2:
        return dict(trend=0, event=None)
    h1, h2, l1, l2 = hi[-2][1], hi[-1][1], lo[-2][1], lo[-1][1]
    tr = 1 if (h2 > h1 and l2 > l1) else (-1 if (h2 < h1 and l2 < l1) else 0)
    p, ev = c[-1]["c"], None
    if p > h2:
        ev, tr = ("BOS↑" if tr == 1 else "CHoCH↑"), 1
    elif p < l2:
        ev, tr = ("BOS↓" if tr == -1 else "CHoCH↓"), -1
    return dict(trend=tr, event=ev)


def bull_zones(c, a, tf):
    """Zones haussieres non mitigees : FVG (imbalance) et Order Blocks (derniere bougie baissiere avant displacement)."""
    n, out, seen = len(c), [], set()
    for i in range(max(2, n - 120), n):
        lo, hi = c[i - 2]["h"], c[i]["l"]
        if hi - lo >= 0.15 * a and all(c[j]["l"] > lo for j in range(i + 1, n)):
            out.append(dict(k="FVG " + tf, lo=lo, hi=hi, w=TFW[tf]))
    for i in range(max(6, n - 120), n):
        body = abs(c[i]["c"] - c[i]["o"])
        if c[i]["c"] > c[i]["o"] and body >= 0.9 * a and c[i]["c"] > max(x["h"] for x in c[i - 5:i]):
            for j in range(i - 1, i - 5, -1):
                if c[j]["c"] < c[j]["o"]:
                    lo, hi = c[j]["l"], c[j]["h"]
                    if j not in seen and all(c[m]["c"] > lo for m in range(j + 1, n)):
                        seen.add(j)
                        out.append(dict(k="OB " + tf, lo=lo, hi=hi, w=TFW[tf]))
                    break
    return out


def vwap_calc(f, t0):
    """VWAP ancre sur l'ouverture de session (18:00 NY) + ecart-type."""
    day = [x for x in f if x["t"] >= t0]
    if len(day) < 6:
        return None
    tp = lambda x: (x["h"] + x["l"] + x["c"]) / 3
    sv = sum(x["v"] for x in day)
    if not sv:
        return None
    vw = sum(tp(x) * x["v"] for x in day) / sv
    sd = math.sqrt(sum(x["v"] * (tp(x) - vw) ** 2 for x in day) / sv)
    return vw, sd


def profile(f, bins=40, tick=0.25):
    """Volume profile : POC, VAH, VAL (70 % du volume)."""
    if len(f) < 6:
        return None
    top, bot = max(x["h"] for x in f), min(x["l"] for x in f)
    B = max(tick, (top - bot) / bins) if top > bot else tick
    prof = {}
    for x in f:
        a, b = int(x["l"] / B), int(x["h"] / B)
        for k in range(a, b + 1):
            prof[k] = prof.get(k, 0) + x["v"] / (b - a + 1)
    tot = sum(prof.values())
    if not tot:
        return None
    poc = max(prof, key=prof.get)
    lo = hi = poc
    acc, kmin, kmax = prof[poc], min(prof), max(prof)
    while acc < 0.7 * tot and (lo > kmin or hi < kmax):
        up = prof.get(hi + 1, 0) if hi < kmax else -1
        dn = prof.get(lo - 1, 0) if lo > kmin else -1
        if up >= dn:
            hi += 1
            acc += max(up, 0)
        else:
            lo -= 1
            acc += max(dn, 0)
    return dict(poc=(poc + 0.5) * B, vah=(hi + 1) * B, val=lo * B)


def prev_rth(c5, tick):
    """Profil de la derniere seance reguliere complete (09:30-16:00 NY)."""
    end0 = last_anchor(16)
    for k in range(7):
        e = end0 - timedelta(days=k)
        s_ = e.replace(hour=9, minute=30)
        bars = [x for x in c5 if s_.timestamp() <= x["t"] < e.timestamp()]
        if len(bars) >= 20:
            p = profile(bars, 40, tick)
            if p:
                p["hi"] = max(x["h"] for x in bars)
                p["lo"] = min(x["l"] for x in bars)
                return p
    return None


def window(h0, h1):
    s_ = last_anchor(h0)
    dur = ((h1 - h0) % 24) or 24
    return s_.timestamp(), (s_ + timedelta(hours=dur)).timestamp()


def hl_range(c, a, b):
    r = [x for x in c if a <= x["t"] < b]
    return (max(x["h"] for x in r), min(x["l"] for x in r)) if len(r) >= 3 else None


def liquidity(D, prth):
    """Pools de liquidite (prix, nom) : EQH/EQL, swings 1H/4H, PDH/PDL, PWH/PWL, PMH/PML, Asie, Londres, RTH J-1."""
    pool = []
    for tf, pre in (("1h", "1H"), ("4h", "4H"), ("15m", "15m")):
        c = D.get(tf)
        if not c or len(c) < 30:
            continue
        a = atr(c)
        hi, lo = swings(c[-300:], 3)
        for pts, kind in ((hi, "High"), (lo, "Low")):
            for _, p in pts[-12:]:
                e = sum(1 for _, q in pts if abs(q - p) <= 0.1 * a) >= 2
                if tf != "15m" or e:
                    nm = (("EQL " if kind == "Low" else "EQH ") + pre) if e else (kind + " " + pre)
                    pool.append((p, nm))
    for tf, h, l in (("1d", "PDH", "PDL"), ("1w", "PWH", "PWL"), ("1M", "PMH", "PML")):
        c = D.get(tf)
        if c and len(c) >= 2:
            pool += [(c[-2]["h"], h), (c[-2]["l"], l)]
    asia = hl_range(D["5m"], *window(20, 0))
    lon = hl_range(D["5m"], *window(2, 5))
    if asia:
        pool += [(asia[0], "Asia High"), (asia[1], "Asia Low")]
    if lon:
        pool += [(lon[0], "London High"), (lon[1], "London Low")]
    if prth:
        pool += [(prth["hi"], "RTH J-1 High"), (prth["lo"], "RTH J-1 Low")]
    return pool


def nearby(pool, refs, price):
    m = {}
    for p, k in list(pool) + [(p, n) for n, p in refs]:
        m.setdefault(round(p, 1), []).append(k)
    up, dn = [], []
    for p in sorted(m):
        names = ", ".join(dict.fromkeys(m[p]))
        (up if p > price else dn).append({"p": round(p, 2), "k": names})
    return {"above": up[:5], "below": dn[-5:][::-1]}


# ====================== 5. SESSIONS / ADR ======================

def session_ctx(d1):
    ny = ny_now()
    h = ny.hour + ny.minute / 60
    kz, name = None, "hors session"
    if 2 <= h < 5:
        kz = name = "Killzone Londres"
    elif 7 <= h < 9.5:
        kz = name = "Pre-ouverture NY (8h30)"
    elif 9.5 <= h < 11:
        kz = name = "Killzone NY AM (ouverture)"
    elif 11 <= h < 13.5:
        name = "Midi NY (faible probabilite)"
    elif 13.5 <= h < 16:
        kz = name = "Killzone NY PM"
    elif h >= 20 or h < 0.5:
        name = "Asie (accumulation)"
    sb = (3 <= h < 4) or (10 <= h < 11) or (14 <= h < 15)
    adr, adr_pct = None, None
    if len(d1) > 12:
        adr = sum(x["h"] - x["l"] for x in d1[-11:-1]) / 10
        adr_pct = (d1[-1]["h"] - d1[-1]["l"]) / adr if adr else None
    return dict(kz=kz, name=name, sb=sb, ny=ny.strftime("%H:%M NY"), adr=adr,
                adr_pct=round(adr_pct, 2) if adr_pct is not None else None)


# ====================== 6. INTERMARKET (VIX, US10Y, DXY) + SMT ======================

def inter(d):
    """Pour un indice, le contexte favorable est : VIX, taux et dollar en baisse (achat) ou en hausse (vente)."""
    items, states = [], []
    for sym, name in (("^VIX", "VIX"), ("^TNX", "US10Y"), ("DX-Y.NYB", "DXY")):
        try:
            x = cached("im" + sym, 240, lambda s=sym: yahoo(s, "5d", "15m"))
            cl = [b["c"] for b in x]
            if len(cl) < 25:
                raise ValueError
            ma = sum(cl[-20:]) / 20
            above, rising = cl[-1] > ma, cl[-1] > cl[-7]
            dr = 1 if (above and rising) else (-1 if (not above and not rising) else 0)
            st = 0 if dr == 0 else (1 if dr == -d else -1)
            states.append(st)
            items.append("%s %.2f : %s moyenne 15m, %s (%s)" % (
                name, cl[-1], "au-dessus" if above else "sous", "hausse" if rising else "baisse",
                "confirme" if st == 1 else ("contre" if st == -1 else "neutre")))
        except Exception:
            items.append(name + " indisponible")
    state_ = "CONTRE" if states.count(-1) >= 2 or (-1 in states and 1 not in states) else (
        "CONFIRME" if states.count(1) >= 2 or (1 in states and -1 not in states) else "NEUTRE")
    return state_, items


def smt(own, oth, d):
    """Divergence SMT NQ/ES : un indice casse un extreme recent (bas si d=1, haut si d=-1), l'autre non."""
    if not oth or len(own) < 60 or len(oth) < 60:
        return None

    def new_ext(c):
        if d == 1:
            return min(x["l"] for x in c[-8:]) < min(x["l"] for x in c[-48:-8])
        return max(x["h"] for x in c[-8:]) > max(x["h"] for x in c[-48:-8])
    return new_ext(own) != new_ext(oth)


# ====================== 7. SWEEP + MSS (ICT) ======================

def find_sweep(c, levels, tf):
    """Balayage de liquidite sous un niveau (espace 'achat') puis MSS (cassure du dernier lower-high)."""
    n, best = len(c), None
    for i in range(max(2, n - 24), n):
        for p, name in levels:
            if c[i]["l"] < p and (c[i]["c"] > p or any(c[j]["c"] > p for j in range(i + 1, min(n, i + 3)))):
                if not best or i > best[0] or (i == best[0] and p > best[1]):
                    best = (i, p, name)
    if not best:
        return None
    i, p, name = best
    il = min(range(max(0, i - 1), min(n, i + 3)), key=lambda j: c[j]["l"])
    hi, _ = swings(c, 2)
    pre = [q for j, q in hi if j < il]
    mss = bool(pre) and any(c[j]["c"] > pre[-1] for j in range(il + 1, n))
    return dict(name=name, level=p, low=c[il]["l"], mss=mss, tf=tf, age=n - 1 - i)


# ====================== 8. AMD : A -> M -> D (POC de A) ======================

def amd_side(cs, ai, ls, buf):
    """Detecte, en espace 'achat' (barres signees), le sweep du bas de A (ls) puis le MSS."""
    n = len(cs)
    i0 = next((i for i in range(ai, n) if cs[i]["l"] < ls - buf), None)
    if i0 is None:
        return None

    def mss_level(ei):
        pre = cs[max(0, ei - 60):ei]
        if len(pre) < 5:
            return None
        hs, _ = swings(pre, 2)
        return hs[-1][1] if hs else max(x["h"] for x in pre[-6:])

    ei, lvl, mss_i = i0, mss_level(i0), None
    for i in range(i0 + 1, n):
        if cs[i]["l"] < cs[ei]["l"]:
            ei, lvl, mss_i = i, mss_level(i), None
        elif mss_i is None and lvl is not None and cs[i]["c"] > lvl:
            mss_i = i
    rec = any(cs[j]["c"] > ls for j in range(i0, n))
    far = max([cs[j]["h"] for j in range(ei + 1, n)] or [cs[ei]["h"]])
    return dict(i0=i0, ei=ei, ext=cs[ei]["l"], lvl=lvl, mss_i=mss_i, rec=rec, far=far,
                accepted=(not rec and n - i0 >= 6))


def amd_detect(c5, price, a15, tick, sess):
    sH, eH = [int(v) for v in AMD_A.split("-")]
    ny = ny_now()
    a0 = last_anchor(sH, ny)
    dur = ((eH - sH) % 24) or 24
    a1 = a0 + timedelta(hours=dur)
    end = (a0 + timedelta(days=1 if sH >= 16 else 0)).replace(hour=16, minute=0)
    t0, t1, te = a0.timestamp(), a1.timestamp(), end.timestamp()
    base = dict(phase="INDISPONIBLE", label="", s=0, a=None, mss=False, rec=False, swept=None, ext=None,
                far=None, mss_level=None, prog=0, poc_reached=False, kz=False, a0=int(t0), a1=int(t1),
                cycle_end=te, window="%02d:00-%02d:00 NY" % (sH, eH))
    if now_ts() >= te:
        base.update(phase="CYCLE_TERMINE",
                    label="Cycle AMD termine (16:00 NY). Prochaine accumulation a %02d:00 NY." % sH)
        return base
    cut = min(now_ts(), t1)
    ab = [x for x in c5 if t0 <= x["t"] < cut]
    if len(ab) < 6:
        base["label"] = "Accumulation indisponible : pas assez de barres 5m (marche ferme ?)."
        return base
    hi, lo = max(x["h"] for x in ab), min(x["l"] for x in ab)
    pr = profile(ab, 30, tick) or dict(poc=(hi + lo) / 2, vah=hi, val=lo)
    adr = sess.get("adr")
    A = dict(hi=hi, lo=lo, poc=pr["poc"], vah=pr["vah"], val=pr["val"], rng=hi - lo,
             ratio=round((hi - lo) / adr, 2) if adr else None)
    base["a"] = A
    if now_ts() < t1:
        base.update(phase="A_EN_COURS", label="Accumulation en cours (%s) : range provisoire %.0f pts, POC A %.2f." % (
            base["window"], A["rng"], A["poc"]))
        return base
    cx = [x for x in c5 if x["t"] < te]
    ai = next((i for i, x in enumerate(cx) if x["t"] >= t1), None)
    if ai is None:
        base.update(phase="A_TERMINEE", label="A terminee : High %.2f / Low %.2f / POC %.2f. Attente d'une manipulation." % (
            hi, lo, A["poc"]))
        return base
    buf = max(tick, 0.05 * a15)
    cands = []
    for s in (1, -1):
        cs = flip(cx, s)
        ls = A["lo"] if s == 1 else -A["hi"]
        r = amd_side(cs, ai, ls, buf)
        if r:
            r.update(s=s, ls=ls, us=(A["hi"] if s == 1 else -A["lo"]), poc_s=s * A["poc"], px_s=cs[-1]["c"])
            cands.append(r)
    if not cands:
        base.update(phase="A_TERMINEE", label="A terminee : High %.2f / Low %.2f / POC %.2f. Attente d'un sweep de A (manipulation)." % (
            hi, lo, A["poc"]))
        return base
    cd = max(cands, key=lambda r: (r["mss_i"] is not None, r["ei"]))
    s, ext, far = cd["s"], cd["ext"], cd["far"]
    mss = cd["mss_i"] is not None
    pocr, oppr = far >= cd["poc_s"], far >= cd["us"]
    side = "Low" if s == 1 else "High"
    dname = "haussiere" if s == 1 else "baissiere"
    if cd["accepted"] and not mss:
        ph, lab = "BREAKOUT", "Prix accepte hors du range A (%s) : cassure, pas de manipulation." % side
    elif not mss:
        ph, lab = "M_EN_COURS", "Manipulation : sweep du %s de A a %.2f. Attente du MSS pour valider une distribution %s." % (
            side, s * ext, dname)
    elif oppr:
        ph, lab = "D_TERMINEE", "Distribution %s terminee : extreme oppose de A atteint." % dname
    elif pocr:
        ph, lab = "D_POC_ATTEINT", "Distribution %s : POC de A atteint (%.2f). Retest du POC possible, puis extreme oppose de A." % (
            dname, A["poc"])
    else:
        ph, lab = "D_EN_COURS", "Distribution %s en cours apres sweep du %s de A : cible POC de A %.2f." % (
            dname, side, A["poc"])
    sh = datetime.fromtimestamp(cx[cd["ei"]]["t"], NY)
    hh = sh.hour + sh.minute / 60
    path = cd["poc_s"] - ext
    prog = round(max(0.0, min(1.5, (cd["px_s"] - ext) / path)) * 100) if path > 0 else 0
    base.update(phase=ph, label=lab, s=s, mss=mss, rec=cd["rec"], swept="A " + side, ext=s * ext, far=s * far,
                mss_level=(s * cd["lvl"]) if cd["lvl"] is not None else None, prog=prog, poc_reached=pocr,
                kz=(2 <= hh < 5 or 7 <= hh < 11 or 13.5 <= hh < 16),
                ext_s=ext, far_s=far, poc_s=cd["poc_s"], opp_s=cd["us"], ls=cd["ls"],
                ext_t=cx[cd["ei"]]["t"])
    return base


def amd_plan(am, X):
    """Plan AMD en espace 'achat' (s=+1 sweep du bas, s=-1 sweep du haut). Retourne (plan, raison)."""
    s, a15, tick = am["s"], X["a15"], X["tick"]
    ext, far, poc, opp = am["ext_s"], am["far_s"], am["poc_s"], am["opp_s"]
    price = s * X["price"]
    leg = far - ext
    if leg <= 0.3 * a15:
        return None, "distribution trop faible apres le MSS"
    buf = max(2 * tick, 0.3 * a15)
    r = (far - price) / leg
    retest = am["phase"] == "D_POC_ATTEINT"
    if retest:
        if price < poc - 0.3 * a15:
            return None, "prix repasse sous le POC de A : retest invalide"
        imm = price - poc <= 0.3 * a15
        entry = price if imm else poc
        sl = max(ext - buf, poc - 1.5 * a15)
        mode = "IMMEDIATE : retest du POC de A" if imm else "LIMITE : attendre le retest du POC de A"
        z_lo, z_hi, kind = poc - 0.1 * a15, poc + 0.1 * a15, "POC A (retest)"
    else:
        if r > 0.92:
            return None, "retracement trop profond (structure compromise)"
        if r >= 0.45:
            entry, imm, mode = price, True, "IMMEDIATE : retracement apres MSS"
        else:
            entry, imm, mode = far - 0.705 * leg, False, "LIMITE : attendre l'OTE 62-79 %"
        sl = ext - buf
        z_lo, z_hi, kind = far - 0.79 * leg, far - 0.62 * leg, "OTE AMD"
    tg = []
    if not retest:
        tg.append((poc, "POC A"))
    tg.append((opp, "A High" if s == 1 else "A Low"))
    tg.append((am["a"]["vah"] if s == 1 else -am["a"]["val"], "VAH A" if s == 1 else "VAL A"))
    tg += [(s * p, n) for p, n in X["liq"]]
    tg += [(s * p, n) for n, p in X["refs"] if n in TGT_REF]
    tg = sorted((p, n) for p, n in tg if p > entry + 0.5 * a15)
    ded = []
    for p, n in tg:
        if ded and p - ded[-1][0] < 0.15 * a15:
            continue
        ded.append((p, n))
    tg = ded
    risk = entry - sl
    if not tg or risk < max(0.25 * a15, 4 * tick) or risk > 5 * a15:
        return None, "stop trop serre/large ou aucune cible"
    if retest:
        k = next((i for i, (p, n) in enumerate(tg) if (p - entry) / risk >= AMD_MIN_RR), 0)
    else:
        k = next((i for i, (p, n) in enumerate(tg) if n == "POC A"), 0)
    tp1 = tg[k]
    tp2 = tg[k + 1] if k + 1 < len(tg) else None
    rr1 = (tp1[0] - entry) / risk
    rr2 = (tp2[0] - entry) / risk if tp2 else None
    if not (rr1 >= AMD_MIN_RR or (rr2 is not None and rr2 >= MIN_RR)):
        return None, "R:R insuffisant (TP1 %.1f)" % rr1
    lo_r, hi_r = sorted((s * z_lo, s * z_hi))
    return {
        "dir": s, "mode": mode, "src": "AMD", "imm": imm, "entry": s * entry, "sl": s * sl,
        "tp": s * tp1[0], "tp_kind": tp1[1], "rr": round(rr1, 2),
        "tp2": s * tp2[0] if tp2 else None, "tp2_kind": tp2[1] if tp2 else None,
        "rr2": round(rr2, 2) if rr2 is not None else None,
        "poi": {"k": kind, "lo": round(lo_r, 2), "hi": round(hi_r, 2), "conf": ["POC A %.2f" % (s * poc)]},
        "pts_sl": round(risk), "pts_tp": round(tp1[0] - entry), "t": now_ts(),
    }, ""


def amd_eval(am, X):
    ph = am["phase"]
    res = dict(ok=False, plan=None, score=None, max=None, checks=[], why="")
    if ph not in ("M_EN_COURS", "D_EN_COURS", "D_POC_ATTEINT"):
        return res
    s, a15 = am["s"], X["a15"]
    plan, why = (None, "MSS non confirme") if ph == "M_EN_COURS" else amd_plan(am, X)
    tr = X["tr"]
    al = sum(1 for t in ("4h", "1d") if tr.get(t, 0) == s)
    ct = sum(1 for t in ("4h", "1d") if tr.get(t, 0) == -s)
    pools = [s * p for p, n in X["liq"] if not n.startswith("A ")]
    ls = am["ls"]
    took_pool = any(am["ext_s"] - 0.1 * a15 <= q < ls - 0.1 * a15 for q in pools)
    A = am["a"]
    checks = [
        ("A propre : range %.0f pts%s" % (A["rng"], (" (%d%% ADR)" % round(A["ratio"] * 100)) if A["ratio"] else ""),
         1 if (A["ratio"] is not None and A["ratio"] <= 0.8) else 0, 1),
        ("Sweep de %s + retour dans le range" % am["swept"], 2 if am["rec"] else 0, 2),
        ("MSS apres le sweep", 2 if am["mss"] else 0, 2),
        ("Displacement >= 1 ATR(15m)", 1 if (am["far_s"] - am["ext_s"]) >= a15 else 0, 1),
        ("Biais combine aligne avec la distribution", 2 if X["bias"] == s else (-2 if X["bias"] == -s else 0), 2),
        ("Structure 4H + Daily alignee", 1 if al == 2 else (-1 if ct == 2 else 0), 1),
        ("Sweep realise en killzone", 1 if am["kz"] else 0, 1),
        ("Divergence SMT NQ/ES", 1 if X["smt"].get(s) else 0, 1),
        ("Intermarket (VIX, US10Y, DXY)", 1 if X["ist_by"][s] == "CONFIRME" else (-1 if X["ist_by"][s] == "CONTRE" else 0), 1),
        ("Le sweep prend aussi un pool de liquidite", 1 if took_pool else 0, 1),
        ("Plan valide avec POC de A (R:R)", 1 if plan else 0, 1),
        ("Order flow confirme", 1 if (X["flow_sign"] == s) else 0, 1),
        ("ADR journalier pas epuise", 1 if (X["sess"]["adr_pct"] is not None and X["sess"]["adr_pct"] < 1.0) else 0, 1),
    ]
    score = sum(g for _, g, _ in checks)
    res.update(plan=plan, why=why, score=score, max=sum(m for *_, m in checks),
               checks=[{"label": l, "got": g, "max": m} for l, g, m in checks])
    res["ok"] = bool(plan and plan["imm"] and am["mss"] and am["rec"] and score >= AMD_MIN_SCORE)
    return res


# ====================== 9. SETUP ICT ======================

def make_plan(d, entry, sl, targets, mode, zone, a15, tick):
    risk = entry - sl
    if risk <= 0 or risk < max(0.25 * a15, 4 * tick) or risk > 4 * a15:
        return None
    pick = None
    for i, (t, k) in enumerate(targets):
        if (t - entry) / risk >= MIN_RR:
            pick = i
            break
    if pick is None:
        return None
    tp, tk = targets[pick]
    tp2 = targets[pick + 1] if pick + 1 < len(targets) else None
    return {
        "dir": d, "mode": mode, "src": "ICT", "entry": d * entry, "sl": d * sl, "tp": d * tp, "tp_kind": tk,
        "rr": round((tp - entry) / risk, 2),
        "tp2": d * tp2[0] if tp2 else None, "tp2_kind": tp2[1] if tp2 else None,
        "rr2": round((tp2[0] - entry) / risk, 2) if tp2 else None,
        "poi": zone, "pts_sl": round(risk), "pts_tp": round(tp - entry), "t": now_ts(),
    }


def build_setup(d, D, X):
    price, a15, a1h, tick = d * X["price"], X["a15"], X["a1h"], X["tick"]
    F = {tf: flip(D[tf], d) for tf in D}
    zs = []
    for tf in ("5m", "15m", "1h", "4h", "1d"):
        c = F.get(tf)
        if c and len(c) > 25:
            zs += bull_zones(c, atr(c), tf)
    rl = min(x["l"] for x in F["15m"][-6:])
    zs = [z for z in zs if z["lo"] < price]
    for z in zs:
        z["touched"] = rl <= z["hi"] + 0.1 * a15 and price - z["hi"] <= 2 * a15
        z["dist"] = max(0.0, price - z["hi"])
    zs = [z for z in zs if z["touched"] or z["dist"] <= 3 * a1h]
    if not zs:
        return None
    z = max(zs, key=lambda z: (z["touched"], z["w"], z["hi"]))

    tol = 0.15 * a15
    conf = [n for n, p in X["refs"] if z["lo"] - tol <= d * p <= z["hi"] + tol]
    c1 = F["1h"]
    hi1, _ = swings(c1, 3)
    if hi1:
        ih, ph = hi1[-1]
        seg = c1[max(0, ih - 60):ih]
        if seg:
            R = ph - min(x["l"] for x in seg)
            if ph - 0.79 * R <= z["hi"] + tol and ph - 0.62 * R >= z["lo"] - tol:
                conf.append("OTE 62-79%")

    c4 = F["4h"][-40:]
    top, bot = max(x["h"] for x in c4), min(x["l"] for x in c4)
    pos = (price - bot) / (top - bot) if top > bot else 0.5

    below = [(d * p, n) for p, n in X["liq"] if d * p < price]
    sw = None
    for tf in ("15m", "5m"):
        c = F.get(tf)
        if c and len(c) > 30:
            r = find_sweep(c, below, tf)
            if r and (not sw or (r["mss"] and not sw["mss"])):
                sw = r
    m1 = False
    if F.get("1m") and len(F["1m"]) > 30:
        e = structure(F["1m"], 2)["event"]
        m1 = bool(e and e.endswith("↑"))

    tr = X["tr"]
    al = sum(1 for t in ("4h", "1d") if tr.get(t, 0) == d)
    ct = sum(1 for t in ("4h", "1d") if tr.get(t, 0) == -d)
    htf = 2 if al == 2 else (1 if al == 1 and ct == 0 else (-2 if ct == 2 else 0))
    fb = X["fb"]
    vref = {n: p for n, p in X["refs"]}
    va = None
    if "VAL" in vref and "VAH" in vref:
        va = min(d * vref["VAL"], d * vref["VAH"])
    am = X["amd"]
    if am["s"] == d and am["mss"]:
        amd_g, amd_t = 2, "AMD aligne : sweep de %s + MSS" % am["swept"]
    elif am["s"] == d and am["swept"]:
        amd_g, amd_t = 1, "AMD : sweep de %s sans MSS" % am["swept"]
    elif am["s"] == -d and am["mss"]:
        amd_g, amd_t = -2, "AMD CONTRE (distribution inverse)"
    else:
        amd_g, amd_t = 0, "AMD : %s" % am["phase"].replace("_", " ").lower()
    checks = [
        ("Structure 4H + Daily alignees", htf, 2),
        ("Biais fondamental aligne", 2 if fb == d else (-2 if fb == -d else 0), 2),
        ("Prix en discount (achat) / premium (vente) 4H", 1 if pos < 0.5 else 0, 1),
        ("POI touchee : %s" % z["k"], (2 if z["w"] >= 2 else 1) if z["touched"] else 0, 2),
        ("Confluences POI : %s" % (", ".join(conf) if conf else "aucune"), min(2, len(conf)), 2),
        ("Sweep de liquidite%s" % (" : %s (%s)" % (sw["name"], sw["tf"]) if sw else ""), 2 if sw else 0, 2),
        ("MSS / CHoCH sur LTF apres le sweep", 2 if (sw and sw["mss"]) else 0, 2),
        ("Confirmation 1m (BOS/CHoCH)", 1 if m1 else 0, 1),
        ("Killzone / Silver Bullet : %s" % (X["sess"]["kz"] or "hors session"),
         (1 if X["sess"]["kz"] else 0) + (1 if X["sess"]["sb"] else 0), 2),
        ("Intermarket VIX + US10Y + DXY", 1 if X["ist"] == "CONFIRME" else (-2 if X["ist"] == "CONTRE" else 0), 1),
        ("Order flow (COT + delta futures)", 1 if X["conf"] == "CONFIRME" else (-1 if X["conf"] == "CONTRE" else 0), 1),
        ("Prix hors/au bord de la value area", 1 if (va is not None and price <= va + tol) else 0, 1),
        ("ADR journalier pas epuise", 1 if (X["sess"]["adr_pct"] is not None and X["sess"]["adr_pct"] < 1.0) else 0, 1),
        ("Divergence SMT NQ/ES", 1 if X["smt"].get(d) else 0, 1),
        (amd_t, amd_g, 2),
    ]
    score = sum(g for _, g, _ in checks)

    buf = max(0.3 * a15, 2 * tick)
    if z["touched"]:
        entry, mode = price, "IMMEDIATE : reaction sur la POI"
        sl = min(z["lo"], rl, sw["low"] if sw else z["lo"]) - buf
    else:
        entry, mode = (z["lo"] + z["hi"]) / 2, "LIMITE (previsionnel) : attendre la POI"
        sl = min(z["lo"], sw["low"] if sw else z["lo"]) - buf
    tg = [(d * p, n) for p, n in X["liq"] if d * p > entry + 0.5 * a15]
    tg += [(d * p, n) for n, p in X["refs"] if n in TGT_REF and d * p > entry + 0.5 * a15]
    seen, tgs = [], []
    for p, n in sorted(tg):
        if seen and p - seen[-1] < 0.15 * a15:
            continue
        seen.append(p)
        tgs.append((p, n))
    lo_r, hi_r = sorted((d * z["lo"], d * z["hi"]))
    zone = {"k": z["k"], "lo": round(lo_r, 2), "hi": round(hi_r, 2), "conf": conf}
    plan = make_plan(d, entry, sl, tgs, mode, zone, a15, tick)
    return {"zone": zone, "touched": z["touched"], "dist_pts": round(z["dist"]), "checks": [
        {"label": l, "got": g, "max": m} for l, g, m in checks], "score": score, "max": sum(m for *_, m in checks),
        "grade": "A+" if score >= 18 else ("A" if score >= MIN_SCORE else "B"),
        "sweep": ({"name": sw["name"], "level": round(d * sw["level"], 2), "mss": sw["mss"], "tf": sw["tf"],
                   "age": sw["age"]} if sw else None), "plan": plan}


# ====================== MOTEUR ======================

def analyse(sym):
    S = IDX[sym]
    tick = S["tick"]
    ST = state(sym)
    names = ["1m", "5m", "15m", "1h", "1d", "1w", "1M"]
    with ThreadPoolExecutor(7) as ex:
        res = list(ex.map(lambda n: safe_tf(sym, n), names))
    D = {n: r for n, r in zip(names, res) if r}
    for need in ("5m", "15m", "1h", "1d", "1w", "1M"):
        if need not in D:
            raise RuntimeError("Donnees %s indisponibles (Yahoo)" % need)
    D["4h"] = resample(D["1h"], 14400, anchor4h())
    c5, c15 = D["5m"], D["15m"]
    price = D["1m"][-1]["c"] if "1m" in D else c5[-1]["c"]
    closed = now_ts() - c15[-1]["t"] > 5400

    fund = cached("fund" + sym, 3600, lambda: fundamentals(sym, D["1d"]))
    flow = [cached("cot" + sym, 10800, lambda: cot_item(sym))] + delta_items(c5)
    try:
        news = cached("cal", 900, calendar)
    except Exception:
        news = []
    oth15 = safe_tf(S["other"], "15m")

    ny = ny_now()
    sess0 = last_anchor(18, ny).timestamp()
    refs = []
    vv = vwap_calc(c5, sess0)
    if vv:
        refs += [("VWAP", vv[0]), ("VWAP+1s", vv[0] + vv[1]), ("VWAP-1s", vv[0] - vv[1]),
                 ("VWAP+2s", vv[0] + 2 * vv[1]), ("VWAP-2s", vv[0] - 2 * vv[1])]
    cur = profile([x for x in c5 if x["t"] >= sess0], 60, tick)
    if cur:
        refs += [("POC", cur["poc"]), ("VAH", cur["vah"]), ("VAL", cur["val"])]
    prth = prev_rth(c5, tick)
    if prth:
        refs += [("POC J-1", prth["poc"]), ("VAH J-1", prth["vah"]), ("VAL J-1", prth["val"])]
    refs += [("Open jour", D["1d"][-1]["o"]), ("Open semaine", D["1w"][-1]["o"]), ("Open mois", D["1M"][-1]["o"])]
    for nm, tt in (("Open NY 00:00", ny.replace(hour=0, minute=0, second=0, microsecond=0)),
                   ("Open 09:30 NY", ny.replace(hour=9, minute=30, second=0, microsecond=0))):
        if tt.timestamp() <= now_ts():
            b = next((x for x in c5 if x["t"] >= tt.timestamp()), None)
            if b and b["t"] - tt.timestamp() < 900:
                refs.append((nm, b["o"]))

    mtf = []
    for tf in ORDER:
        c = D.get(tf)
        if not c or len(c) < 25:
            continue
        a = atr(c)
        st = structure(c, 2 if tf in ("1m", "5m") else 3)
        seg = c[-LOOK[tf]:]
        top, bot = max(x["h"] for x in seg), min(x["l"] for x in seg)
        pos = (price - bot) / (top - bot) if top > bot else 0.5
        bz = [z for z in bull_zones(c, a, tf) if z["lo"] < price]
        sz = [z for z in bull_zones(flip(c, -1), a, tf) if z["lo"] < -price]
        sup = max(bz, key=lambda z: z["hi"]) if bz else None
        res_ = max(sz, key=lambda z: z["hi"]) if sz else None
        mtf.append({"tf": tf, "trend": st["trend"], "event": st["event"], "pd": round(pos * 100),
                    "sup": ("%s %.1f-%.1f" % (sup["k"].split()[0], sup["lo"], sup["hi"])) if sup else None,
                    "res": ("%s %.1f-%.1f" % (res_["k"].split()[0], -res_["hi"], -res_["lo"])) if res_ else None})
    tr = {m["tf"]: m["trend"] for m in mtf}

    ftotal = round(sum(i["score"] * i["w"] for i in fund) / sum(i["w"] for i in fund) * 100)
    htf = (tr.get("1w", 0) + 2 * tr.get("1d", 0) + 2 * tr.get("4h", 0)) / 5 * 100
    comb = round(0.5 * ftotal + 0.5 * htf)
    bias, fb = sign(comb, BIAS_TH), sign(ftotal, BIAS_TH)
    ofb = sign(sum(i["score"] for i in flow), 1)
    conf = "NEUTRE" if (ofb == 0 or bias == 0) else ("CONFIRME" if ofb == bias else "CONTRE")
    news_risk = any(-30 <= n["minutes"] <= 90 for n in news)
    sess = session_ctx(D["1d"])
    IST = {1: inter(1), -1: inter(-1)}
    ist, itxt = IST[bias if bias else 1]
    smtv = {1: smt(c15, oth15, 1), -1: smt(c15, oth15, -1)}

    a15, a1h = atr(c15), atr(D["1h"])
    am = amd_detect(c5, price, a15, tick, sess)
    liq = liquidity(D, prth)
    if am["a"]:
        A = am["a"]
        refs += [("POC A", A["poc"]), ("VAH A", A["vah"]), ("VAL A", A["val"])]
        liq += [(A["hi"], "A High"), (A["lo"], "A Low")]
    X = dict(price=price, a15=a15, a1h=a1h, tick=tick, refs=refs, liq=liq, tr=tr, fb=fb, bias=bias,
             sess=sess, ist=ist, conf=conf, amd=am, smt=smtv, flow_sign=ofb,
             ist_by={1: IST[1][0], -1: IST[-1][0]})
    ar = amd_eval(am, X)

    plan, setup, status, msg = ST["plan"], None, "NEUTRE", ""
    if plan:
        d = plan["dir"]
        hit_sl = price <= plan["sl"] if d == 1 else price >= plan["sl"]
        hit_tp = price >= plan["tp"] if d == 1 else price <= plan["tp"]
        is_amd = plan.get("src") == "AMD"
        reason = None
        if hit_tp:
            log(sym, "TP atteint (%.2f)" % price)
            ST["plan"] = plan = None
            status, msg = "OBJECTIF ATTEINT", "TP touche : scenario termine."
        elif not is_amd and bias == -d:
            reason = "le biais combine s'est inverse"
        elif hit_sl:
            reason = "niveau d'invalidation casse (%.2f)" % price
        elif not is_amd and inter(d)[0] == "CONTRE":
            reason = "VIX / US10Y / DXY passes contre le scenario"
        elif now_ts() - plan["t"] > 8 * 3600 or (is_amd and now_ts() >= am["cycle_end"]):
            reason = "scenario expire"
        if reason:
            log(sym, "STOP : " + reason)
            ST["plan"] = plan = None
            status, msg = "STOP", "Scenario arrete : " + reason
        elif plan:
            status, msg = "EN COURS", "Le scenario %s suit toujours l'analyse." % plan.get("src", "ICT")

    amd_note = (" AMD : " + am["label"]) if am["label"] else ""
    if not plan and status not in ("STOP", "OBJECTIF ATTEINT"):
        if closed:
            status, msg = "MARCHE FERME", "Pas de donnees recentes (week-end ?)."
        elif news_risk:
            status, msg = "PAUSE NEWS", "News majeure USD proche : pas de nouvelle entree."
        elif ar["ok"]:
            ST["plan"] = plan = ar["plan"]
            status = "SETUP CONFIRMED"
            msg = "AMD %s : sweep de A + MSS, score %d/%d, cible POC de A." % (
                "ACHAT" if am["s"] == 1 else "VENTE", ar["score"], ar["max"])
            log(sym, "SETUP AMD %s @ %.2f (R:R %.2f, score %d)" % (
                "ACHAT" if am["s"] == 1 else "VENTE", plan["entry"], plan["rr"], ar["score"]))
        elif bias == 0:
            status, msg = "NEUTRE", "Biais combine neutre (%+d) : pas de trade ICT.%s" % (comb, amd_note)
        else:
            setup = build_setup(bias, D, X)
            if not setup:
                status, msg = "ATTENTE", "Biais present mais aucune POI exploitable pres du prix.%s" % amd_note
            elif not setup["plan"]:
                status, msg = "ATTENTE", "POI trouvee mais aucun R:R >= %.1f (ou SL trop large).%s" % (MIN_RR, amd_note)
            else:
                sw = setup["sweep"]
                ok = setup["touched"] and sw and sw["mss"] and setup["score"] >= MIN_SCORE and ist != "CONTRE"
                if ok:
                    ST["plan"] = plan = setup["plan"]
                    status, msg = "SETUP CONFIRMED", "Score %d/%d (%s) : POI + sweep + MSS alignes." % (
                        setup["score"], setup["max"], setup["grade"])
                    log(sym, "SETUP ICT %s @ %.2f (R:R %.2f, score %d)" % (
                        "ACHAT" if bias == 1 else "VENTE", plan["entry"], plan["rr"], setup["score"]))
                elif setup["touched"]:
                    status, msg = "REACTION", "POI touchee : score %d/%d, il manque sweep/MSS ou score (min %d)." % (
                        setup["score"], setup["max"], MIN_SCORE)
                else:
                    status, msg = "SURVEILLANCE", "Biais %s. POI %s a %d pts du prix (score %d/%d).%s" % (
                        "haussier" if bias == 1 else "baissier", setup["zone"]["k"], setup["dist_pts"],
                        setup["score"], setup["max"], amd_note)

    lines = []
    if am["a"]:
        lines += [("A High", am["a"]["hi"], "#3b82f6"), ("A Low", am["a"]["lo"], "#3b82f6"),
                  ("POC A", am["a"]["poc"], "#eab308")]
    if vv:
        lines.append(("VWAP", vv[0], "#a78bfa"))
    pl = plan or ar["plan"]
    if pl:
        lines += [("Entree", pl["entry"], "#e6e9ee"), ("SL", pl["sl"], "#ef4444"), ("TP1", pl["tp"], "#22c55e")]
    chart = {"bars": [[x["t"], x["o"], x["h"], x["l"], x["c"]] for x in c5[-170:]],
             "lines": [{"n": n, "p": round(p, 2), "c": c} for n, p, c in lines], "a0": am["a0"], "a1": am["a1"]}
    amd_out = {"phase": am["phase"], "label": am["label"], "dir": am["s"], "window": am["window"], "a": am["a"],
               "swept": am["swept"], "ext": am["ext"], "far": am["far"], "mss": am["mss"], "rec": am["rec"],
               "mss_level": am["mss_level"], "prog": am["prog"], "poc_reached": am["poc_reached"],
               "score": ar["score"], "max": ar["max"], "checks": ar["checks"], "why": ar["why"],
               "preview": ar["plan"], "min_score": AMD_MIN_SCORE}

    return {"sym": sym, "name": S["name"], "price": round(price, 2), "source": "Yahoo Finance (futures %s)" % S["y"],
            "closed": closed, "status": status, "message": msg,
            "bias": {"dir": bias, "comb": comb, "fund": ftotal, "htf": round(htf)},
            "fund": fund, "flow": flow, "conf": conf, "inter": {"state": ist, "items": itxt},
            "smt": {"buy": smtv[1], "sell": smtv[-1]},
            "sess": sess, "refs": [{"n": n, "p": round(p, 2)} for n, p in refs], "mtf": mtf,
            "pool": nearby(liq, refs, price), "setup": setup, "plan": plan, "amd": amd_out, "chart": chart,
            "news": news[:6], "log": ST["log"],
            "updated": datetime.fromtimestamp(now_ts(), timezone.utc).strftime("%H:%M:%S UTC")}


@app.route("/api/analysis")
def api():
    sym = (request.args.get("s") or DEFAULT_SYM).upper()
    if sym not in IDX:
        sym = DEFAULT_SYM
    try:
        with LOCK:
            return jsonify(analyse(sym))
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/health")
def health():
    return "ok"


@app.route("/")
def index():
    return Response(PAGE.replace("__DEFAULT__", DEFAULT_SYM), mimetype="text/html")


PAGE = r"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>INDEX ICT + AMD Engine</title>
<style>
:root{--bg:#0e1116;--card:#171c24;--tx:#e6e9ee;--mut:#8b94a3;--g:#22c55e;--r:#ef4444;--y:#eab308;--b:#3b82f6}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:14px system-ui,sans-serif;padding:12px;max-width:720px;margin:auto}
.card{background:var(--card);border-radius:12px;padding:14px;margin-bottom:12px}
h2{margin:0 0 8px;font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.05em}
.top{display:flex;justify-content:space-between;align-items:center;gap:8px}.price{font-size:28px;font-weight:700}
.badge{padding:6px 12px;border-radius:20px;font-weight:700;font-size:13px;text-align:center;background:#334}
.row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #222a35;gap:8px}
.row:last-child{border:0}.s{font-weight:700;min-width:34px;text-align:right}
.mut{color:var(--mut);font-size:12px}button{background:var(--b);color:#fff;border:0;border-radius:8px;padding:8px 12px}
button.off{background:#2a3240;color:var(--mut)}
table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:4px 3px;border-bottom:1px solid #222a35;text-align:left}th{color:var(--mut);font-weight:600}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.kv{background:#10151c;border-radius:8px;padding:8px}.kv b{display:block;font-size:16px}
.bar{height:8px;border-radius:4px;background:#222a35;overflow:hidden;margin:6px 0}.bar i{display:block;height:100%;background:var(--y)}
</style></head><body>
<div style="display:flex;gap:8px;margin-bottom:12px"><button id="b-NQ" onclick="pick('NQ')">NQ</button><button id="b-ES" onclick="pick('ES')">ES</button></div>
<div class="card"><div class="top"><div><div class="mut" id="nm">...</div><div class="price" id="price">...</div></div>
<div class="badge" id="status">Chargement</div></div>
<p id="msg" style="margin:10px 0 0"></p><div class="mut" id="upd"></div></div>
<div class="card" id="planCard" style="display:none"><h2>Plan de trade</h2><div id="plan"></div></div>
<div class="card"><h2>AMD : accumulation - manipulation - distribution</h2><div id="amd"></div></div>
<div class="card"><h2>Graphique 5m</h2><canvas id="cv" style="width:100%;height:260px"></canvas></div>
<div class="card"><h2>Biais</h2><div id="bias"></div></div>
<div class="card"><h2>Checklist ICT (score de confluence)</h2><div id="setup"></div></div>
<div class="card"><h2>Multi-timeframe (1m a mensuel)</h2><div style="overflow-x:auto"><table id="mtf"></table></div></div>
<div class="card"><h2>Niveaux : liquidite, VWAP, Volume Profile</h2><div class="grid"><div id="above"></div><div id="below"></div></div><div id="refs" class="mut" style="margin-top:8px"></div></div>
<div class="card"><h2>Fondamental (pondere)</h2><div id="fund"></div></div>
<div class="card"><h2>Order flow, intermarket &amp; SMT</h2><div id="flow"></div></div>
<div class="card"><h2>Session</h2><div id="sess"></div></div>
<div class="card"><h2>News USD (impact eleve)</h2><div id="news"></div></div>
<div class="card"><h2>Journal</h2><div id="log"></div></div>
<button onclick="load()">Actualiser</button>
<p class="mut">Outil d'aide a la decision, pas un conseil financier. A tester en demo.</p>
<script>
const $=id=>document.getElementById(id);
let SYM=(localStorage.getItem('sym')||'__DEFAULT__');
const col=s=>s>0?'var(--g)':(s<0?'var(--r)':'var(--mut)');
const f=(x,n)=>x==null?'-':Number(x).toFixed(n==null?2:n);
const arrow=t=>t>0?'<span style="color:var(--g)">&#9650;</span>':(t<0?'<span style="color:var(--r)">&#9660;</span>':'<span class="mut">&#9679;</span>');
const SC={'SETUP CONFIRMED':'var(--g)','EN COURS':'var(--g)','OBJECTIF ATTEINT':'var(--g)','STOP':'var(--r)','REACTION':'var(--y)','SURVEILLANCE':'var(--b)','PAUSE NEWS':'var(--y)'};
function rows(a,fn){return a.length?a.map(fn).join(''):'<div class="mut">-</div>'}
function pick(s){SYM=s;try{localStorage.setItem('sym',s)}catch(e){}load()}
function tabs(){['NQ','ES'].forEach(s=>$('b-'+s).className=(s==SYM?'':'off'))}
function chart(ch){
 const cv=$('cv'),dpr=window.devicePixelRatio||1,W=cv.clientWidth,H=cv.clientHeight;
 cv.width=W*dpr;cv.height=H*dpr;const g=cv.getContext('2d');g.scale(dpr,dpr);g.clearRect(0,0,W,H);
 const B=ch.bars;if(!B.length)return;
 let hi=Math.max(...B.map(b=>b[2])),lo=Math.min(...B.map(b=>b[3]));const pad=(hi-lo)*.5;
 const L=ch.lines.filter(l=>l.p>lo-pad&&l.p<hi+pad);L.forEach(l=>{hi=Math.max(hi,l.p);lo=Math.min(lo,l.p)});
 const m=(hi-lo)*.04;hi+=m;lo-=m;const RW=58,cw=(W-RW)/B.length;
 const y=p=>H-(p-lo)/(hi-lo)*H,x=i=>i*cw+cw/2;
 const ia=B.findIndex(b=>b[0]>=ch.a0);
 if(ia>=0){const ib=B.findIndex(b=>b[0]>=ch.a1);const ah=L.find(l=>l.n=='A High'),al=L.find(l=>l.n=='A Low');
  if(ah&&al){g.fillStyle='rgba(59,130,246,.13)';g.fillRect(x(ia)-cw/2,y(ah.p),((ib<0?B.length:ib)-ia)*cw,y(al.p)-y(ah.p))}}
 B.forEach((b,i)=>{const up=b[4]>=b[1];g.strokeStyle=g.fillStyle=up?'#22c55e':'#ef4444';
  g.beginPath();g.moveTo(x(i),y(b[2]));g.lineTo(x(i),y(b[3]));g.stroke();
  g.fillRect(x(i)-cw*.35,Math.min(y(b[1]),y(b[4])),cw*.7,Math.max(1,Math.abs(y(b[1])-y(b[4]))))});
 g.font='10px system-ui';L.forEach(l=>{g.strokeStyle=g.fillStyle=l.c;g.setLineDash([4,3]);g.beginPath();g.moveTo(0,y(l.p));g.lineTo(W-RW,y(l.p));g.stroke();g.setLineDash([]);
  g.fillText(l.n+' '+l.p.toFixed(1),W-RW+3,y(l.p)+3)});
}
function render(r){
 tabs();$('nm').textContent=r.name;
 $('price').textContent=f(r.price);
 const st=$('status');st.textContent=r.status;st.style.background=SC[r.status]||'#334';
 $('msg').textContent=r.message;$('upd').textContent='Maj '+r.updated+' - '+r.source;
 const p=r.plan;$('planCard').style.display=p?'block':'none';
 if(p){const l=p.dir==1?'ACHAT':'VENTE';
  $('plan').innerHTML='<div style="font-weight:700;color:'+(p.dir==1?'var(--g)':'var(--r)')+'">'+l+' '+p.src+' - '+p.mode+'</div><div class="mut">POI '+p.poi.k+' '+f(p.poi.lo)+'-'+f(p.poi.hi)+(p.poi.conf.length?' + '+p.poi.conf.join(', '):'')+'</div>'
  +'<div class="grid" style="margin-top:8px"><div class="kv">Entree<b>'+f(p.entry)+'</b></div><div class="kv">Stop<b style="color:var(--r)">'+f(p.sl)+'</b><span class="mut">'+p.pts_sl+' pts</span></div>'
  +'<div class="kv">TP1 ('+p.tp_kind+')<b style="color:var(--g)">'+f(p.tp)+'</b><span class="mut">'+p.pts_tp+' pts - R:R '+p.rr+'</span></div>'
  +'<div class="kv">TP2'+(p.tp2?' ('+p.tp2_kind+')':'')+'<b>'+(p.tp2?f(p.tp2):'-')+'</b><span class="mut">'+(p.rr2?'R:R '+p.rr2:'')+'</span></div></div>';}
 const a=r.amd,A=a.a;let h='<div class="row"><b>'+a.phase.replace(/_/g,' ')+'</b><span class="mut">fenetre A '+a.window+'</span></div><div class="mut" style="margin:6px 0">'+a.label+'</div>';
 if(A){h+='<div class="grid"><div class="kv">A High<b>'+f(A.hi)+'</b></div><div class="kv">A Low<b>'+f(A.lo)+'</b></div><div class="kv">POC A<b style="color:var(--y)">'+f(A.poc)+'</b></div><div class="kv">VAH / VAL A<b>'+f(A.vah,1)+' / '+f(A.val,1)+'</b><span class="mut">range '+Math.round(A.rng)+' pts'+(A.ratio?' ('+Math.round(A.ratio*100)+'% ADR)':'')+'</span></div></div>'}
 if(a.dir){h+='<div class="mut" style="margin-top:8px">Chemin vers le POC de A : '+a.prog+'%'+(a.poc_reached?' (atteint)':'')+'</div><div class="bar"><i style="width:'+Math.min(100,a.prog)+'%"></i></div>'
  +'<div class="row"><span>Sweep</span><b>'+a.swept+' a '+f(a.ext)+(a.mss?' - MSS '+f(a.mss_level):' - sans MSS')+'</b></div>'}
 if(a.checks.length){h+='<div class="row" style="margin-top:6px"><b>Score AMD '+a.score+'/'+a.max+'</b><span class="mut">min '+a.min_score+(a.why?' - '+a.why:'')+'</span></div>'
  +a.checks.map(c=>'<div class="row"><span>'+c.label+'</span><span class="s" style="color:'+col(c.got)+'">'+c.got+'/'+c.max+'</span></div>').join('')}
 if(a.preview&&!p){const q=a.preview;h+='<div class="mut" style="margin-top:8px">Plan potentiel ('+(q.dir==1?'ACHAT':'VENTE')+', '+q.mode+') : entree '+f(q.entry)+' - SL '+f(q.sl)+' - TP1 '+f(q.tp)+' ('+q.tp_kind+', R:R '+q.rr+')</div>'}
 $('amd').innerHTML=h;chart(r.chart);
 const b=r.bias;
 $('bias').innerHTML='<div class="row"><span>Biais combine</span><span class="s" style="color:'+col(b.dir)+'">'+(b.dir>0?'HAUSSIER':(b.dir<0?'BAISSIER':'NEUTRE'))+' ('+(b.comb>0?'+':'')+b.comb+')</span></div>'
  +'<div class="row"><span>Fondamental</span><span class="s" style="color:'+col(b.fund)+'">'+(b.fund>0?'+':'')+b.fund+'</span></div>'
  +'<div class="row"><span>Structure HTF (W, D, 4H)</span><span class="s" style="color:'+col(b.htf)+'">'+(b.htf>0?'+':'')+b.htf+'</span></div>';
 const s=r.setup;
 $('setup').innerHTML=s?('<div class="row"><b>Score '+s.score+'/'+s.max+' - grade '+s.grade+'</b><span class="mut">POI '+s.zone.k+' '+f(s.zone.lo)+'-'+f(s.zone.hi)+'</span></div>'
  +rows(s.checks,c=>'<div class="row"><span>'+c.label+'</span><span class="s" style="color:'+col(c.got)+'">'+c.got+'/'+c.max+'</span></div>')):'<div class="mut">Aucun setup ICT en cours d\'analyse.</div>';
 $('mtf').innerHTML='<tr><th>TF</th><th>Tend.</th><th>Event</th><th>P/D</th><th>Support (OB/FVG)</th><th>Resistance</th></tr>'
  +r.mtf.map(m=>'<tr><td><b>'+m.tf+'</b></td><td>'+arrow(m.trend)+'</td><td>'+(m.event||'-')+'</td><td style="color:'+(m.pd>50?'var(--r)':'var(--g)')+'">'+m.pd+'%</td><td>'+(m.sup||'-')+'</td><td>'+(m.res||'-')+'</td></tr>').join('');
 $('above').innerHTML='<div class="mut">Au-dessus</div>'+rows(r.pool.above,x=>'<div class="row"><span>'+x.k+'</span><b>'+f(x.p)+'</b></div>');
 $('below').innerHTML='<div class="mut">En-dessous</div>'+rows(r.pool.below,x=>'<div class="row"><span>'+x.k+'</span><b>'+f(x.p)+'</b></div>');
 $('refs').textContent=r.refs.map(x=>x.n+' '+f(x.p)).join(' | ');
 $('fund').innerHTML=rows(r.fund,i=>'<div class="row"><span>'+i.label+' <span class="mut">(x'+i.w+')</span></span><span class="s" style="color:'+col(i.score)+'">'+(i.score>0?'+':'')+i.score+'</span></div>');
 const sm=r.smt;
 $('flow').innerHTML=rows(r.flow,i=>'<div class="row"><span>'+i.label+'</span><span class="s" style="color:'+col(i.score)+'">'+(i.score>0?'+':'')+i.score+'</span></div>')
  +'<div class="row"><span>Order flow vs biais</span><span class="s">'+r.conf+'</span></div>'
  +'<div class="row"><span>Intermarket</span><span class="s">'+r.inter.state+'</span></div>'+r.inter.items.map(t=>'<div class="mut">'+t+'</div>').join('')
  +'<div class="row"><span>Divergence SMT NQ/ES (achat / vente)</span><span class="s">'+(sm.buy==null?'-':(sm.buy?'oui':'non'))+' / '+(sm.sell==null?'-':(sm.sell?'oui':'non'))+'</span></div>';
 const se=r.sess;
 $('sess').innerHTML='<div class="row"><span>Heure</span><b>'+se.ny+'</b></div><div class="row"><span>Session</span><b>'+se.name+(se.sb?' - Silver Bullet':'')+'</b></div>'
  +'<div class="row"><span>ADR journalier consomme</span><b>'+(se.adr_pct==null?'-':Math.round(se.adr_pct*100)+'%')+'</b></div>';
 $('news').innerHTML=rows(r.news,n=>'<div class="row"><span>'+n.country+' - '+n.title+'</span><span class="mut">'+n.time+'</span></div>');
 $('log').innerHTML=rows(r.log,l=>'<div class="row"><span>'+l.msg+'</span><span class="mut">'+l.t+'</span></div>');
}
async function load(){
 tabs();
 try{const r=await (await fetch('/api/analysis?s='+SYM)).json();
  if(r.error){$('msg').textContent='Erreur : '+r.error;return}
  render(r)}catch(e){$('msg').textContent='Erreur reseau'}
}
load();setInterval(load,60000);
</script></body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
