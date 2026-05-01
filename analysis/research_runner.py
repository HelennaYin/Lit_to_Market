"""Custom weekly publication-momentum research runner.

This module mirrors the weekly analysis from ``Logic_test`` but is written for
web API use: no notebook prints, no plot files, and JSON-friendly output.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

import numpy as np
import pandas as pd
import requests
from scipy import stats


OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_SLEEP = 0.15
REQUEST_TIMEOUT = 30
VALID_SIGNALS = ("pub_deviation", "pub_zscore", "pub_4w_dev")
LAGS = list(range(-4, 13))
CAR_WINDOWS = [1, 2, 4, 8, 12]


@dataclass
class ResearchInput:
    run_id: str
    keywords: list[str]
    ticker: str
    date_start: date
    date_end: date
    mailto: str = "research@litmarket.io"


ProgressCallback = Callable[[str, int, str], None]


def run_research(input_data: ResearchInput, progress: ProgressCallback | None = None) -> dict:
    """Run a custom weekly publication/market analysis."""
    notify = progress or (lambda _stage, _pct, _message: None)

    notify("publications", 10, "Fetching weekly publication counts from OpenAlex")
    publications = fetch_weekly_publications(
        input_data.keywords,
        input_data.date_start,
        input_data.date_end,
        input_data.mailto,
    )

    notify("market", 42, f"Fetching {input_data.ticker} and SPY market data")
    sector_weekly, spy_weekly = fetch_weekly_market(
        input_data.ticker,
        input_data.date_start,
        input_data.date_end,
    )

    notify("signals", 62, "Computing publication signals and abnormal returns")
    merged, market_model = build_analysis_frame(publications, sector_weekly, spy_weekly)

    notify("statistics", 76, "Running lag correlation, Granger, and CAR tests")
    signals = {
        signal: analyze_signal(merged, signal, input_data.ticker)
        for signal in VALID_SIGNALS
    }

    notify("complete", 100, "Research run complete")
    return {
        "run_id": input_data.run_id,
        "ticker": input_data.ticker,
        "keywords": input_data.keywords,
        "date_range": [input_data.date_start.isoformat(), input_data.date_end.isoformat()],
        "market_model": market_model,
        "series": json_records(merged),
        "signals": signals,
    }


def fetch_weekly_publications(
    keywords: list[str],
    start: date,
    end: date,
    mailto: str,
) -> pd.DataFrame:
    rows = []
    title_query = "|".join(_clean_openalex_value(keyword) for keyword in keywords)

    for index, (week_start, from_date, to_date) in enumerate(week_windows(start, end)):
        params = {
            "filter": (
                f"title.search:{title_query},"
                f"from_publication_date:{from_date},"
                f"to_publication_date:{to_date}"
            ),
            "per-page": 1,
            "select": "id",
            "mailto": mailto,
        }
        data = openalex_get(params)
        rows.append(
            {
                "week_start": pd.Timestamp(week_start),
                "pub_count": int(data.get("meta", {}).get("count", 0)),
            }
        )
        time.sleep(OPENALEX_SLEEP)

    return pd.DataFrame(rows)


def openalex_get(params: dict, retries: int = 5) -> dict:
    wait = 5
    for attempt in range(retries):
        try:
            response = requests.get(
                f"{OPENALEX_BASE}/works",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(wait)
                wait *= 2
                continue
            if not response.ok:
                raise RuntimeError(f"OpenAlex {response.status_code}: {response.text[:180]}")
            return response.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"OpenAlex request failed: {exc}") from exc
            time.sleep(wait)
            wait *= 2
    raise RuntimeError("OpenAlex API unavailable after retries")


def fetch_weekly_market(ticker: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    import yfinance as yf

    padded_start = start - timedelta(days=10)
    padded_end = end + timedelta(days=7)
    raw = yf.download(
        [ticker, "SPY"],
        start=padded_start.isoformat(),
        end=padded_end.isoformat(),
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=False,
    )
    if raw.empty:
        raise RuntimeError("No market data returned from yfinance")

    sector_daily = normalize_yfinance(raw, ticker, "custom", ticker)
    spy_daily = normalize_yfinance(raw, "SPY", "spy", "SPY")
    if sector_daily.empty or spy_daily.empty:
        raise RuntimeError("Ticker or SPY data was missing from yfinance response")

    return weekly_returns(sector_daily), weekly_returns(spy_daily, return_name="spy_return")


def normalize_yfinance(raw: pd.DataFrame, ticker: str, sector: str, label: str) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0):
            df = raw[ticker].copy()
        elif len(raw.columns.levels) > 1 and ticker in raw.columns.get_level_values(1):
            df = raw.xs(ticker, level=1, axis=1).copy()
        else:
            return pd.DataFrame()
    else:
        df = raw.copy()

    close_col = "Adj Close" if "Adj Close" in df.columns else "Close"
    required = {"Open", "High", "Low", close_col}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df.index),
            "open": df["Open"],
            "high": df["High"],
            "low": df["Low"],
            "close": df[close_col],
            "volume": df["Volume"] if "Volume" in df.columns else np.nan,
            "sector": sector,
            "ticker": label,
        }
    ).dropna(subset=["close"])
    out = out.sort_values("date").reset_index(drop=True)
    out["log_return"] = np.log(out["close"] / out["close"].shift(1))
    return out.dropna(subset=["log_return"])


def weekly_returns(df_daily: pd.DataFrame, return_name: str = "log_return") -> pd.DataFrame:
    df = df_daily.copy()
    df["week_start"] = (df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")).dt.normalize()
    weekly = (
        df.groupby(["sector", "ticker", "week_start"], as_index=False)
        .agg(close=("close", "last"))
        .sort_values(["sector", "week_start"])
    )
    weekly[return_name] = weekly.groupby("sector")["close"].transform(lambda s: np.log(s / s.shift(1)))
    return weekly.dropna(subset=[return_name]).reset_index(drop=True)


def build_analysis_frame(
    publications: pd.DataFrame,
    sector_weekly: pd.DataFrame,
    spy_weekly: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    from statsmodels.regression.linear_model import OLS
    from statsmodels.tools import add_constant

    sector_returns = sector_weekly[["week_start", "ticker", "log_return"]]
    spy_returns = spy_weekly[["week_start", "spy_return"]]
    returns = pd.merge(sector_returns, spy_returns, on="week_start", how="inner").dropna()
    if len(returns) < 104:
        raise RuntimeError("Need at least two years of weekly market observations")

    model_df = returns[["log_return", "spy_return"]].dropna()
    model = OLS(model_df["log_return"], add_constant(model_df["spy_return"])).fit()
    alpha = float(model.params["const"])
    beta = float(model.params["spy_return"])
    returns["abnormal_return"] = returns["log_return"] - (alpha + beta * returns["spy_return"])

    pubs = publications.sort_values("week_start").copy()
    pubs["pub_deviation"] = pubs["pub_count"] - pubs["pub_count"].median()
    rolling_mean = pubs["pub_count"].rolling(52, min_periods=26).mean()
    rolling_std = pubs["pub_count"].rolling(52, min_periods=26).std()
    pubs["pub_zscore"] = (pubs["pub_count"] - rolling_mean) / rolling_std
    rolling_sum = pubs["pub_count"].rolling(4, min_periods=1).sum()
    pubs["pub_4w_dev"] = rolling_sum - rolling_sum.median()

    merged = pd.merge(pubs, returns, on="week_start", how="inner")
    if len(merged.dropna(subset=["abnormal_return"])) < 104:
        raise RuntimeError("Need at least two years of overlapping publication and market observations")

    market_model = {
        "alpha": round(alpha, 8),
        "beta": round(beta, 6),
        "r_squared": round(float(model.rsquared), 6),
        "n_obs": int(len(model_df)),
    }
    return merged, market_model


def analyze_signal(df: pd.DataFrame, signal_col: str, ticker: str) -> dict:
    clean = df[["week_start", "pub_count", signal_col, "abnormal_return"]].dropna().copy()
    signal = clean[signal_col]
    outcome = clean["abnormal_return"]
    dates = clean["week_start"]

    lag_corr = run_lag_correlation(signal, outcome, LAGS)
    if lag_corr.empty:
        raise RuntimeError(f"Insufficient data for lag correlation on {signal_col}")
    best_corr = lag_corr.loc[lag_corr["pearson_p"].idxmin()].to_dict()
    best_lag = int(best_corr["lag"])
    rolling = run_rolling_correlation(signal, outcome, best_lag, 52, dates)
    granger = run_granger(signal, outcome, 6)
    car, event_idx, threshold = run_car(signal, outcome, CAR_WINDOWS)

    return {
        "sector": "custom_research",
        "ticker": ticker,
        "signal_col": signal_col,
        "label": signal_col,
        "n_obs": int(len(clean)),
        "date_range": [dates.min().date().isoformat(), dates.max().date().isoformat()],
        "adf": {
            "signal": run_adf(signal, signal_col),
            "outcome": run_adf(outcome, "abnormal_return"),
        },
        "lag_correlation": lag_corr.to_dict("records"),
        "best_lag_corr": json_clean(best_corr),
        "rolling_correlation": {
            "window": 52,
            "lag_used": best_lag,
            "mean_r": safe_round(rolling.dropna().mean(), 4),
            "pct_positive": safe_round((rolling.dropna() > 0).mean() * 100, 2),
            "points": json_records(
                pd.DataFrame(
                    {
                        "week_start": rolling.index,
                        "rolling_r": rolling.values,
                    }
                )
            ),
        },
        "granger": granger.to_dict("records") if not granger.empty else [],
        "car": car.to_dict("records") if not car.empty else [],
        "car_n_events": int(len(event_idx)),
        "car_threshold": safe_float(threshold),
    }


def run_adf(series: pd.Series, name: str) -> dict:
    from statsmodels.tsa.stattools import adfuller

    clean = series.dropna()
    if len(clean) < 20:
        return {"name": name, "stat": None, "p_value": None, "is_stationary": None, "n": int(len(clean))}
    result = adfuller(clean, autolag="AIC")
    return {
        "name": name,
        "stat": safe_round(result[0], 4),
        "p_value": safe_round(result[1], 4),
        "is_stationary": bool(result[1] < 0.05),
        "n": int(len(clean)),
    }


def run_lag_correlation(signal: pd.Series, outcome: pd.Series, lags: list[int]) -> pd.DataFrame:
    bonf_thresh = 0.05 / len(lags)
    rows = []
    for lag in lags:
        x = signal.values
        y = outcome.shift(-lag).values if lag >= 0 else outcome.shift(abs(lag)).values
        n = min(len(x), len(y))
        x, y = x[:n], y[:n]
        mask = ~(np.isnan(x) | np.isnan(y))
        xc, yc = x[mask], y[mask]
        if len(xc) < 10:
            continue
        pearson_r, pearson_p = stats.pearsonr(xc, yc)
        spearman_r, spearman_p = stats.spearmanr(xc, yc)
        rows.append(
            {
                "lag": int(lag),
                "pearson_r": safe_round(pearson_r, 4),
                "pearson_p": safe_round(pearson_p, 4),
                "spearman_r": safe_round(spearman_r, 4),
                "spearman_p": safe_round(spearman_p, 4),
                "n_obs": int(len(xc)),
                "sig_bonf": bool(pearson_p < bonf_thresh),
                "sig_05": bool(pearson_p < 0.05),
                "agree_direction": bool(np.sign(pearson_r) == np.sign(spearman_r)),
                "bonf_thresh": round(bonf_thresh, 5),
            }
        )
    return pd.DataFrame(rows)


def run_rolling_correlation(
    signal: pd.Series,
    outcome: pd.Series,
    lag: int,
    window: int,
    index: pd.Series,
) -> pd.Series:
    y = outcome.shift(-lag) if lag >= 0 else outcome.shift(abs(lag))
    roll = pd.DataFrame({"x": signal.values, "y": y.values})["x"].rolling(window).corr(
        pd.DataFrame({"x": signal.values, "y": y.values})["y"]
    )
    roll.index = index
    return roll


def run_granger(signal: pd.Series, outcome: pd.Series, max_lag: int) -> pd.DataFrame:
    from statsmodels.tsa.stattools import grangercausalitytests

    bonf_thresh = 0.05 / max_lag
    tmp = pd.DataFrame({"outcome": outcome.reset_index(drop=True), "signal": signal.reset_index(drop=True)}).dropna()
    if len(tmp) < max_lag * 4:
        return pd.DataFrame()
    try:
        results = grangercausalitytests(tmp[["outcome", "signal"]], maxlag=max_lag, verbose=False)
    except Exception:
        return pd.DataFrame()
    rows = []
    for lag, result in results.items():
        f_stat = result[0]["ssr_ftest"][0]
        p_value = result[0]["ssr_ftest"][1]
        rows.append(
            {
                "lag": int(lag),
                "f_stat": safe_round(f_stat, 4),
                "p_value": safe_round(p_value, 4),
                "sig_bonf": bool(p_value < bonf_thresh),
                "sig_05": bool(p_value < 0.05),
                "bonf_thresh": round(bonf_thresh, 5),
            }
        )
    return pd.DataFrame(rows)


def run_car(
    signal: pd.Series,
    abnormal_return: pd.Series,
    post_windows: list[int],
    event_threshold_pct: float = 0.75,
    min_gap: int = 12,
) -> tuple[pd.DataFrame, list[int], float]:
    sig_vals = signal.values
    ar_vals = abnormal_return.values
    threshold = float(np.nanquantile(sig_vals, event_threshold_pct))
    event_indices = []
    last_event = -999
    for idx, value in enumerate(sig_vals):
        if not np.isnan(value) and value > threshold and idx - last_event >= min_gap:
            event_indices.append(idx)
            last_event = idx

    rows = []
    for window in post_windows:
        cars = []
        for idx in event_indices:
            end_idx = idx + window + 1
            if end_idx <= len(ar_vals):
                window_ar = ar_vals[idx + 1:end_idx]
                if not np.any(np.isnan(window_ar)):
                    cars.append(window_ar.sum())
        if len(cars) < 3:
            continue
        cars_arr = np.array(cars)
        mean_car = float(cars_arr.mean())
        se_car = float(cars_arr.std() / np.sqrt(len(cars_arr)))
        t_stat, p_value = stats.ttest_1samp(cars_arr, 0)
        rows.append(
            {
                "window": int(window),
                "n_events": int(len(cars_arr)),
                "mean_car": safe_round(mean_car, 5),
                "se_car": safe_round(se_car, 5),
                "t_stat": safe_round(t_stat, 3),
                "p_value": safe_round(p_value, 4),
                "significant": bool(p_value < 0.05),
                "ci_lower": safe_round(mean_car - 1.96 * se_car, 5),
                "ci_upper": safe_round(mean_car + 1.96 * se_car, 5),
            }
        )
    return pd.DataFrame(rows), event_indices, threshold


def week_windows(start: date, end: date) -> list[tuple[str, str, str]]:
    current = start - timedelta(days=start.weekday())
    windows = []
    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        windows.append((current.isoformat(), current.isoformat(), week_end.isoformat()))
        current += timedelta(days=7)
    return windows


def _clean_openalex_value(value: str) -> str:
    return value.strip().replace(",", " ")


def safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def safe_round(value, digits: int) -> float | None:
    value = safe_float(value)
    return round(value, digits) if value is not None else None


def json_clean(value):
    if isinstance(value, dict):
        return {key: json_clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_clean(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return safe_float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float):
        return safe_float(value)
    return value


def json_records(df: pd.DataFrame) -> list[dict]:
    rows = []
    for row in df.to_dict("records"):
        rows.append(json_clean(row))
    return rows
