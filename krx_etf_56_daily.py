#!/usr/bin/env python3
"""KRX 공식 ETF 일봉: 58개 관심종목 MACD·RSI 분석.

Google Colab / Python 3.10+ / 표준 라이브러리만 사용합니다.
    python krx_etf_56_daily.py --self-test
    python krx_etf_56_daily.py --end 2026-11-25
    python krx_etf_56_daily.py --end 2026-11-25 --out krx_etf_56_output

개인 인증키는 코드나 파일에 기록하지 않고 환경변수 KRX_AUTH_KEY로 전달합니다.
기본 조회창은 종료일을 포함한 과거 420일입니다. 기존 daily.csv가 있으면
확인된 거래일 이후부터 조회하므로 일일 재실행 비용이 줄어듭니다.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


URL = "https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd"
TARGETS = {
    "148020": "RISE 200",
    "396500": "TIGER 반도체TOP10",
    "367760": "RISE 네트워크인프라",
    "139270": "TIGER 200 금융",
    "0148J0": "TIGER 코리아휴머노이드로봇산업",
    "469070": "RISE AI&로봇",
    "487240": "KODEX AI전력핵심설비",
    "102970": "KODEX 증권",
    "0167A0": "SOL AI반도체TOP2플러스",
    "138540": "TIGER 현대차그룹플러스",
    "138530": "TIGER LG그룹플러스",
    "0091P0": "TIGER 코리아원자력",
    "364990": "TIGER 게임TOP10",
    "364970": "TIGER 바이오TOP10",
    "244580": "KODEX 바이오",
    "279530": "KODEX 고배당주",
    "117700": "KODEX 건설",
    "143860": "TIGER 헬스케어",
    "469170": "ACE 포스코그룹포커스",
    "0000J0": "PLUS 한화그룹주",
    "228790": "TIGER 화장품",
    "494670": "TIGER 조선TOP10",
    "466920": "SOL 조선TOP3플러스",
    "449450": "PLUS K방산",
    "305720": "KODEX 2차전지산업",
    "462010": "TIGER 2차전지소재Fn",
    "229200": "KODEX 코스닥150",
    "411060": "ACE KRX금현물",
    "475050": "ACE KPOP포커스",
    "445290": "KODEX 로봇액티브",
    "0190C0": "RISE 현대차고정피지컬AI",
    "228800": "TIGER 여행레저",
    "457990": "PLUS 태양광&ESS",
    "139230": "TIGER 200 중공업",
    "091180": "KODEX 자동차",
    "300950": "KODEX 게임산업",
    "139250": "TIGER 200 에너지화학",
    "139260": "TIGER 200 IT",
    "261220": "KODEX WTI원유선물(H)",
    "133690": "TIGER 미국나스닥100",
    "457480": "ACE 테슬라밸류체인액티브",
    "0023A0": "SOL 미국양자컴퓨팅TOP10",
    "381180": "TIGER 미국필라델피아반도체나스닥",
    "456600": "TIME 글로벌AI인공지능액티브",
    "0142D0": "TIGER 미국AI데이터센터TOP4Plus",
    "0173Y0": "KODEX 미국AI광통신네트워크",
    "448540": "ACE 엔비디아채권혼합",
    "465580": "ACE 미국빅테크TOP7 Plus",
    "0053L0": "TIGER 차이나휴머노이드로봇",
    "0123G0": "TIGER 미국AI전력SMR",
    "487230": "KODEX 미국AI전력핵심인프라",
    "0118Z0": "ACE 미국AI테크핵심산업액티브",
    "446770": "ACE 글로벌반도체TOP4 Plus",
    "360750": "TIGER 미국S&P500",
    "0167Z0": "KODEX 미국우주항공",
    "0183J0": "TIGER 미국우주테크",
    "453810": "KODEX 인도Nifty50",
    "245710": "ACE 베트남VN30(합성)",
}

CSV_FIELDS = ("date", "code", "name", "close")
MIN_HISTORY = 120  # MACD(19,39) 초기값 편향을 줄이는 최소 관측치


def number(raw: object) -> float | None:
    try:
        x = float(str(raw).replace(",", "").strip())
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def request_day(day: date, key: str, retries: int = 3) -> list[dict]:
    req = Request(
        URL + "?" + urlencode({"basDd": day.strftime("%Y%m%d")}),
        headers={"AUTH_KEY": key, "Accept": "application/json", "User-Agent": "KRX-ETF-Research/1.0"},
    )
    for attempt in range(retries):
        try:
            with urlopen(req, timeout=25) as response:
                payload = json.load(response)
            if not isinstance(payload, dict) or not isinstance(payload.get("OutBlock_1"), list):
                raise RuntimeError(f"{day}: OutBlock_1 응답 구조 누락")
            return payload["OutBlock_1"]
        except HTTPError as err:
            if err.code in (401, 403):
                raise RuntimeError(f"{day}: HTTP {err.code} 인증키 또는 서비스 이용승인을 확인하세요") from err
            if err.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise RuntimeError(f"{day}: KRX HTTP {err.code}") from err
        except (URLError, TimeoutError, json.JSONDecodeError) as err:
            if attempt == retries - 1:
                raise RuntimeError(f"{day}: 요청 실패 {type(err).__name__}") from err
        time.sleep(1.5 * (attempt + 1))
    return []


def parsed_day(items: list[dict], day: date) -> list[dict]:
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("ISU_CD", "")).strip().upper()
        if code not in TARGETS:
            continue
        price = number(item.get("TDD_CLSPRC"))
        if price is None or price <= 0:
            continue
        raw_date = str(item.get("BAS_DD") or day.strftime("%Y%m%d")).replace("-", "")
        if raw_date != day.strftime("%Y%m%d"):
            raise RuntimeError(f"응답 기준일 불일치: 요청 {day} / 응답 {raw_date}")
        result.append({"date": day.isoformat(), "code": code, "name": TARGETS[code], "close": price})
    return result


def read_cache(path: Path, start: date, end: date) -> dict[tuple[str, str], dict]:
    cache = {}
    if not path.exists():
        return cache
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            code, date_str = row.get("code", "").strip().upper(), row.get("date", "").strip()
            try:
                d = date.fromisoformat(date_str)
            except ValueError:
                continue
            price = number(row.get("close"))
            if code in TARGETS and start <= d <= end and price is not None and price > 0:
                cache[(code, date_str)] = {"date": date_str, "code": code, "name": TARGETS[code], "close": price}
    return cache


def ema(values: list[float], days: int) -> list[float]:
    alpha = 2 / (days + 1)
    out = [values[0]]
    for x in values[1:]:
        out.append(alpha * x + (1 - alpha) * out[-1])
    return out


def rsi_wilder(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gain = sum(max(0, x) for x in deltas[:period]) / period
    loss = sum(max(0, -x) for x in deltas[:period]) / period
    for x in deltas[period:]:
        gain = (gain * (period - 1) + max(0, x)) / period
        loss = (loss * (period - 1) + max(0, -x)) / period
    if loss == 0:
        return 50.0 if gain == 0 else 100.0
    return 100 - 100 / (1 + gain / loss)


def crosses(macd: list[float], signal: list[float]) -> list[tuple[str, int]]:
    events = []
    for i in range(1, len(macd)):
        prev = macd[i - 1] - signal[i - 1]
        cur = macd[i] - signal[i]
        if prev <= 0 < cur:
            events.append(("golden", i))
        elif prev >= 0 > cur:
            events.append(("dead", i))
    return events


def analyze(code: str, rows: list[dict], asof: str) -> dict:
    rows.sort(key=lambda r: r["date"])
    result = {"code": code, "name": TARGETS[code], "count": len(rows), "status": "insufficient_history"}
    if rows:
        result.update(latest_date=rows[-1]["date"], latest_close=rows[-1]["close"])
    if len(rows) < MIN_HISTORY:
        result["reason"] = f"일봉 {len(rows)}개: 최소 {MIN_HISTORY}개 필요"
        return result
    if rows[-1]["date"] != asof:
        result.update(status="stale_data", reason=f"최종 데이터 {rows[-1]['date']} / 기준일 {asof}")
        return result
    prices = [float(x["close"]) for x in rows]
    rsi = rsi_wilder(prices)
    result["rsi14"] = round(rsi, 4) if rsi is not None else None
    indicators = {}
    for fast, slow in ((12, 26), (19, 39)):
        e1, e2 = ema(prices, fast), ema(prices, slow)
        line = [a - b for a, b in zip(e1, e2)]
        sig = ema(line, 9)
        events = crosses(line, sig)
        last = events[-1] if events else None
        gold = next((ev for ev in reversed(events) if ev[0] == "golden"), None)
        recent = last if last and last[1] == len(rows) - 1 else None
        state = {
            "macd": round(line[-1], 6),
            "signal": round(sig[-1], 6),
            "line_vs_signal": "above" if line[-1] > sig[-1] else "below_or_equal",
            "line_vs_zero": "above" if line[-1] > 0 else "below_or_equal",
            "new_cross": recent[0] if recent else "none",
            "last_cross": last[0] if last else "none",
            "delayed_entry_valid": False,
        }
        if last:
            kind, i = last
            state.update(last_cross_date=rows[i]["date"], last_cross_close=prices[i],
                         days_since_cross=len(rows) - 1 - i,
                         gain_since_cross_pct=round((prices[-1] / prices[i] - 1) * 100, 4))
            state["delayed_entry_valid"] = bool(
                kind == "golden" and prices[-1] <= prices[i] * 1.15 and rsi is not None and rsi <= 65
            )
        if gold:
            _, i = gold
            state.update(last_golden_date=rows[i]["date"], last_golden_close=prices[i],
                         days_since_golden=len(rows) - 1 - i,
                         gain_since_golden_pct=round((prices[-1] / prices[i] - 1) * 100, 4),
                         golden_still_active=bool(last and last[0] == "golden"))
        indicators[f"macd_{fast}_{slow}_9"] = state
    result.update(indicators)
    a, b = indicators["macd_12_26_9"], indicators["macd_19_39_9"]
    valid = a["delayed_entry_valid"] or b["delayed_entry_valid"]
    new_gold = a["new_cross"] == "golden" or b["new_cross"] == "golden"
    result["entry_valid"] = bool(valid)
    result["both_macd_above_signal"] = a["line_vs_signal"] == b["line_vs_signal"] == "above"
    if b["new_cross"] == "dead":
        result["status"] = "new_sell_signal"
    elif new_gold:
        result["status"] = "new_buy_signal"
    elif any(x.get("last_cross") == "golden" and x.get("days_since_cross", 99) < 5 for x in (a, b)):
        result["status"] = "recent_5d_golden"
    elif valid:
        result["status"] = "delayed_entry_valid"
    elif any(x.get("last_cross") == "golden" for x in (a, b)):
        result["status"] = "entry_conditions_unmet"
    else:
        result["status"] = "watch"
    return result


def latest_complete_date() -> date:
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    return now.date() if now.hour >= 16 else now.date() - timedelta(days=1)


def write_outputs(out: Path, rows: list[dict], end: date, status: str) -> list[dict]:
    out.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: (r["code"], r["date"]))
    bycode = defaultdict(list)
    for row in rows:
        bycode[row["code"]].append(row)
    trading_days = sorted({row["date"] for row in rows})
    asof = trading_days[-1] if trading_days else None
    analyses = [analyze(code, bycode[code], asof) if asof else
                {"code": code, "name": name, "count": 0, "status": "no_data"}
                for code, name in TARGETS.items()]
    with (out / "etf_56_daily.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    report = {
        "source": "KRX Open API ETF 일별매매정보 (일별 종가, 수정종가 아님)",
        "requested_end": end.isoformat(),
        "analysis_base_date": asof,
        "collected_trading_days": len(trading_days),
        "target_count": len(TARGETS),
        "analyzable_count": sum(x["status"] not in ("insufficient_history", "stale_data", "no_data") for x in analyses),
        "fetch_status": status,
        "analysis": analyses,
    }
    (out / "etf_56_signals.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return analyses


def run_self_test() -> None:
    assert len(TARGETS) == 58, f"종목 개수 {len(TARGETS)} != 58"
    assert "0131V0" not in TARGETS and "0180V0" not in TARGETS
    assert number("12,345") == 12345 and number("-") is None
    assert abs(ema([10, 12], 3)[-1] - 11) < 1e-9
    assert abs(rsi_wilder(list(range(1, 25))) - 100) < 1e-9
    assert crosses([-1, 1], [0, 0]) == [("golden", 1)]
    assert crosses([1, -1], [0, 0]) == [("dead", 1)]
    d = date(2026, 11, 25)
    data = parsed_day([{"ISU_CD": "0173Y0", "BAS_DD": "20261125", "TDD_CLSPRC": "6,360"}], d)
    assert len(data) == 1 and data[0]["close"] == 6360
    print("PASS: 58개 종목 목록·제외 2개·가격·MACD·RSI 테스트")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--self-test", action="store_true", help="네트워크 없이 프로그램 시험")
    p.add_argument("--end", help="조회 종료일 YYYY-MM-DD (기본: 한국시간 마지막 확정일)")
    p.add_argument("--days", type=int, default=420, help="과거 조회 달력일 수 (기본 420)")
    p.add_argument("--out", type=Path, default=Path("krx_etf_56_output"), help="데이터 보관 폴더")
    p.add_argument("--from-csv", type=Path, help="KRX 접속 없이 기존 CSV로만 분석")
    args = p.parse_args()
    if args.self_test:
        run_self_test()
        return 0
    try:
        end = date.fromisoformat(args.end) if args.end else latest_complete_date()
    except ValueError:
        p.error("--end는 YYYY-MM-DD 형식이어야 합니다")
    if end > latest_complete_date():
        p.error("미래 또는 장중 데이터는 조회할 수 없습니다")
    if args.days < 120:
        p.error("MACD·RSI 분석을 위해 --days는 120 이상이어야 합니다")
    start = end - timedelta(days=args.days - 1)
    if args.from_csv:
        cache = read_cache(args.from_csv, start, end)
        fetch_status = "offline_csv"
        print(f"기존 CSV 분석: {len(cache):,}행")
    else:
        key = os.environ.get("KRX_AUTH_KEY", "").strip()
        if not key:
            p.error("KRX_AUTH_KEY가 없습니다. 인증키를 getpass로 입력하세요")
        cache = read_cache(args.out / "etf_56_daily.csv", start, end)
        existing_dates = [date.fromisoformat(day) for (_, day) in cache]
        history_count = defaultdict(int)
        for code, _ in cache:
            history_count[code] += 1
        needs_backfill = [code for code in TARGETS if history_count[code] < MIN_HISTORY]
        if needs_backfill:
            first = start
            print(f"과거 데이터 보강: {len(needs_backfill)}개 종목, 시작일 {first}", flush=True)
        else:
            first = max(existing_dates) + timedelta(days=1) if existing_dates else start
        fetch_status = "complete"
        if first <= end:
            print(f"KRX 조회 {first} ~ {end}: 하루 1회 API 호출로 58개 동시 수집", flush=True)
        else:
            print("이미 수집된 확정일 데이터 사용", flush=True)
        attempted = 0
        for offset in range((end - first).days + 1):
            day = first + timedelta(days=offset)
            if day.weekday() >= 5:
                continue
            try:
                daily = parsed_day(request_day(day, key), day)
            except RuntimeError as exc:
                # 진행 중 끊겨도 받은 데이터는 보존하고 실패를 표기합니다.
                fetch_status = "partial_failure"
                print(f"요청 중단: {exc}", file=sys.stderr, flush=True)
                break
            for row in daily:
                cache[(row["code"], row["date"])] = row
            attempted += 1
            if attempted % 20 == 0:
                print(f"조회 진행: {attempted}영업일, 누적 일봉 {len(cache):,}개", flush=True)
            time.sleep(0.25)
    results = write_outputs(args.out, list(cache.values()), end, fetch_status)
    counts = defaultdict(int)
    for item in results:
        counts[item["status"]] += 1
    print(f"대상 {len(TARGETS)}개, 분석 가능 {sum(x['status'] not in ('insufficient_history', 'stale_data', 'no_data') for x in results)}개")
    print("상태별: " + ", ".join(f"{k} {v}개" for k, v in sorted(counts.items())))
    print(f"저장 완료: {args.out.resolve()}")
    if fetch_status != "complete":
        print(f"주의: API 데이터 수집이 완전히 끝나지 않았습니다 ({fetch_status})")
    return 0 if fetch_status in ("complete", "offline_csv") else 1


if __name__ == "__main__":
    raise SystemExit(main())
