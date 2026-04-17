# jobs/premarket.py
from __future__ import annotations

import sys
import time
from pathlib import Path

# Ensure repo root is on sys.path so "import src...." works when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataclasses import dataclass
from datetime import datetime
import traceback
from typing import List, Optional, Tuple

from src.common.env import load_env
from src.utility.paths import get_img_dir, get_txt_dir
from src.utility.constant import SECTOR_TICKER_NAME_MAP


# ---------------------------
# Helpers: output + artifacts
# ---------------------------

def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M")


def _reports_dir() -> Path:
    p = Path("data") / "reports"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _graphs_dir_candidates() -> List[Path]:
    # common places we’ve used across branches
    return [
        Path("data") / "reports" / "graphs",
        Path("data") / "reports" / "output_graphs",
        Path("output_graphs"),
    ]


def _write_text_report(filename: str, content: str) -> Path:
    out_dir = get_txt_dir()
    out = out_dir / filename
    out.write_text(content, encoding="utf-8")
    return out


def _find_latest_image() -> Optional[Path]:
    exts = (".png", ".jpg", ".jpeg")
    best: Optional[Path] = None
    best_mtime = -1.0

    d = get_img_dir()
    if d.exists():
        for p in d.glob("*"):
            if p.suffix.lower() not in exts:
                continue
            try:
                m = p.stat().st_mtime
                if m > best_mtime:
                    best_mtime = m
                    best = p
            except Exception:
                continue

    if best is None:
        for d in _graphs_dir_candidates():
            if not d.exists():
                continue
            for p in d.glob("*"):
                if p.suffix.lower() not in exts:
                    continue
                try:
                    m = p.stat().st_mtime
                    if m > best_mtime:
                        best_mtime = m
                        best = p
                except Exception:
                    continue
    return best


def _append_text(textlist: List[str], p: Path):
    # processor expects paths (per your design); store as strings
    textlist.append(str(p))


def _append_img(jpblist: List[str], p: Path):
    jpblist.append(str(p))


# ---------------------------
# Pipeline steps
# ---------------------------

@dataclass
class EquityResult:
    summary_df: object  # pandas DataFrame, but keep loose typing
    vol_spike: List[str]
    recent_abn: List[str]


@dataclass
class OptionsResult:
    summary_df: object  # pandas DataFrame
    iv_spike: List[str]


def pipeline_equity(textlist: List[str], jpblist: List[str], market_date, tickers: Optional[List[str]] = None) -> EquityResult:
    """
    Runs:
      - update universe prices
      - generate equity abnormal-vol report
    Appends a compact text artifact to textlist.
    Pass tickers explicitly after the Reddit step has updated tickers.json.
    """
    from src.prices.polygon_client import save_universe_excel
    from src.prices.smf_process import generate_reports, generate_macro

    print("\n=== [Equity] Updating prices ===")
    updates = save_universe_excel(tickers=tickers, market_date=market_date)

    # Print updates compactly
    if isinstance(updates, dict):
        changed = 0
        for t, info in updates.items():
            mode = info.get("mode", "?")
            rows = info.get("rows", 0)
            if mode != "noop" or rows:
                print(f"{t}: {mode} (+{rows})")
                changed += 1
        if changed == 0:
            print("(no changes)")
    else:
        print(updates)

    print("\n=== [Equity] Generating macro summary ===")
    macro_df = generate_macro(tickers=tickers, asof_date=market_date)
    macro_df['name'] = macro_df['symbol'].map(SECTOR_TICKER_NAME_MAP).fillna('')

    print("\n=== [Equity] Generating report ===")
    summary_df, vol_spike, recent_abn = generate_reports(tickers=tickers, asof_date=market_date)

    # Build a compact text block for the processor
    ts = _now_tag()
    lines = []
    lines.append(f"EQUITY FLAGS — {ts}")
    lines.append("")
    if macro_df is not None and not macro_df.empty:
        lines.append("SECTOR OVERVIEW:")
        if "symbol" in macro_df.columns and "ret_d" in macro_df.columns:
            lines.append(macro_df[["symbol", "name", "ret_d"]].to_string(index=False))
        else:
            lines.append(macro_df.to_string(index=False))
        lines.append("")
    lines.append("VolSpikeFlags: " + (", ".join(vol_spike) if vol_spike else "(none)"))
    lines.append("RecentAbnormalFlags: " + (", ".join(recent_abn) if recent_abn else "(none)"))

    # Optional mini snapshot (safe if columns exist)
    try:
        if summary_df is not None and not summary_df.empty:
            cols = [c for c in ["symbol", "ret_d", "ret_w", "ret_m", "ratio_ewma_vs_21", "ratio_ewma_vs_spy", "z_last_21"]
                    if c in summary_df.columns]
            if cols and "ratio_ewma_vs_21" in summary_df.columns:
                top = summary_df.sort_values("ratio_ewma_vs_21", ascending=False).head(10)
                lines.append("\nTop 10 by ratio_ewma_vs_21:")
                lines.append(top[cols].to_string(index=False))
    except Exception:
        # don’t break the job for formatting
        pass

    p = _write_text_report(f"equity_flags_{ts}.txt", "\n".join(lines))
    _append_text(textlist, p)

    return EquityResult(summary_df=summary_df, vol_spike=vol_spike, recent_abn=recent_abn)


def pipeline_options(textlist: List[str], jpblist: List[str], market_date, tickers: Optional[List[str]] = None) -> OptionsResult:
    """
    Runs:
      - update universe IV series (30D constant-maturity ATM close)
      - generate options IV spike report
    Appends a compact text artifact to textlist.
    Pass tickers explicitly after the Reddit step has updated tickers.json.
    """
    from src.options.options_client import update_atm_iv_series
    from src.options.options_process import generate_options_report

    print("\n=== [Options] Updating IV series ===")
    updates = update_atm_iv_series(tickers=tickers)

    # Print updates compactly
    if isinstance(updates, dict):
        changed = 0
        for t, info in updates.items():
            mode = info.get("mode", "?") if isinstance(info, dict) else "?"
            rows = info.get("rows", 0) if isinstance(info, dict) else int(info)
            if mode != "noop" and rows > 0:
                print(f"{t}: {mode} (+{rows})")
                changed += 1
        if changed == 0:
            print("(no updates)")
    else:
        print(updates)

    print("\n=== [Options] Generating report ===")
    summary_df, iv_spike = generate_options_report(tickers=tickers)

    # Build a compact text block for the processor
    ts = _now_tag()
    lines = []
    lines.append(f"OPTIONS FLAGS — {ts}")
    lines.append("")

    if summary_df is not None and not summary_df.empty:
        lines.append("OPTIONS IV SPIKE SUMMARY:")
        cols = [c for c in ["ticker", "iv_latest", "ewma_iv", "z_iv_21", "iv_ratio_vs_spy", "flag_options_iv_spike"]
                if c in summary_df.columns]
        if cols:
            lines.append(summary_df[cols].to_string(index=False))
        lines.append("")

    lines.append("OptionsIVSpikeFlags: " + (", ".join(iv_spike) if iv_spike else "(none)"))

    p = _write_text_report(f"options_flags_{ts}.txt", "\n".join(lines))
    _append_text(textlist, p)

    return OptionsResult(summary_df=summary_df, iv_spike=iv_spike)


def pipeline_reddit_wisdom(textlist: List[str]) -> None:
    """
    Runs ApeWisdom fetch + dynamic ticker state update.
    Updates data/reddit/ticker_state.csv and data/config/tickers.json.
    """
    from src.reddit.apewisdom import main as apewisdom_main
    from src.reddit.wisdomprocess import main as wisdomprocess_main

    print("\n=== [ApeWisdom] Fetch daily top100 ===")
    apewisdom_main()

    print("\n=== [ApeWisdom] Update dynamic ticker state ===")
    summary = wisdomprocess_main()

    ts = _now_tag()
    lines = [
        f"APEWISDOM TICKER STATE — {ts}",
        "",
        f"date: {summary.get('date')}",
        f"eligible_hot_today: {summary.get('eligible_hot_today')}",
        f"eligible_candidates_today: {summary.get('eligible_candidates_today')}",
        f"active_hot: {summary.get('active_hot')}",
        f"active_candidates: {summary.get('active_candidates')}",
    ]

    p = _write_text_report(f"apewisdom_ticker_state_{ts}.txt", "\n".join(lines))
    _append_text(textlist, p)


def pipeline_news(textlist: List[str]) -> None:
    """
    Fetch GDELT headlines (last 1h, 30 articles), cluster, select top 15.
    Skips fetch if cache is fresh. Falls back to cached files on rate-limit.
    Appends a compact text artifact to textlist.
    """
    from src.news.gdelt import main as gdelt_fetch
    from src.news.gdelt_process import main as gdelt_process
    from datetime import date
    from src.news.gdelt_process import get_output_dir

    out_dir = get_output_dir(date.today())

    print("\n=== [News] Fetching GDELT headlines ===")
    try:
        gdelt_fetch()
    except Exception as exc:
        articles_csv = out_dir / "gdelt_articles.csv"
        if articles_csv.exists():
            print(f"[News] fetch failed ({exc}); using cached articles")
        else:
            print(f"[News] fetch failed and no cache available: {exc}")
            p = _write_text_report(f"news_top15_{_now_tag()}.txt", f"NEWS — (fetch failed, no cache)\n{exc}")
            _append_text(textlist, p)
            return

    print("\n=== [News] Processing top 15 clusters ===")
    try:
        gdelt_process()
    except Exception as exc:
        print(f"[News] processing failed: {exc}")

    top_csv = out_dir / "gdelt_top20.csv"
    ts = _now_tag()
    if top_csv.exists():
        import pandas as pd
        df = pd.read_csv(top_csv)
        lines = [f"NEWS TOP 15 — {ts}", ""]
        for _, row in df.iterrows():
            lines.append(f"[{row.get('latest_time', '')}] {row.get('cluster_headline', '')}")
            lines.append(f"  sources={row.get('source_count', '')} articles={row.get('article_count', '')} url={row.get('sample_url', '')}")
            lines.append("")
        content = "\n".join(lines)
    else:
        content = f"NEWS TOP 15 — {ts}\n\n(no news data available)"

    p = _write_text_report(f"news_top15_{ts}.txt", content)
    _append_text(textlist, p)


def pipeline_reddit_mongo() -> None:
    """
    Runs your friend's reddit->mongo ingestion + VADER sentiment push.
    No artifacts here (those come from the next step that reads Mongo).
    """
    from src.reddit.reddittomongo import (
        SUBREDDIT_NAME,
        POST_FETCH_LIMIT,
        COMMENT_LIMIT,
        load_whitelist,
        fetch_and_store_reddit_posts,
        run_vader_sentiment,
    )

    whitelist = load_whitelist()

    print("\n=== [Reddit] Ingest to Mongo ===")
    fetch_and_store_reddit_posts(SUBREDDIT_NAME, POST_FETCH_LIMIT, COMMENT_LIMIT)

    print("\n=== [Reddit] VADER sentiment ===")
    run_vader_sentiment(SUBREDDIT_NAME, whitelist)


def pipeline_reddit_graph_and_summary(textlist: List[str], jpblist: List[str]):
    """
    Reads Mongo sentiment records, generates a heatmap image, and appends:
      - a text snapshot file to textlist
      - the newest heatmap image to jpblist (if found)
    """
    from src.reddit.sentimentgraph import fetch_df, plot_sentiment_heatmap

    print("\n=== [Reddit] Fetch sentiment DF ===")
    df = fetch_df()
    n = len(df)
    uniq = df["ticker"].nunique() if (n and "ticker" in df.columns) else 0
    print(f"[Reddit] sentiment rows={n}, unique_tickers={uniq}")

    ts = _now_tag()

    if df is None or df.empty:
        p = _write_text_report(f"reddit_snapshot_{ts}.txt", "(no reddit sentiment records available)")
        _append_text(textlist, p)
        return

    # Write a compact “top mentions” snapshot
    try:
        grp = (df.groupby("ticker")
               .agg(mentions=("sentiment_score", "count"),
                    avg_sentiment=("sentiment_score", "mean"))
               .reset_index())
        grp["avg_sentiment"] = grp["avg_sentiment"].round(2)
        grp = grp.sort_values(["mentions", "avg_sentiment"], ascending=[False, False]).head(20)
        snap = grp.to_string(index=False)
    except Exception:
        snap = df.head(50).to_string(index=False)

    p = _write_text_report(f"reddit_snapshot_{ts}.txt", snap)
    _append_text(textlist, p)

    print("\n=== [Reddit] Plot heatmap ===")
    plot_sentiment_heatmap(df)

    # Try to locate latest image (sentimentgraph saves it)
    img = _find_latest_image()
    if img:
        print(f"[Reddit] found heatmap image: {img}")
        _append_img(jpblist, img)
    else:
        print("[Reddit] no heatmap image found to attach.")


# ---------------------------
# Final processor + email
# ---------------------------

def process_and_send(textlist: List[str], jpblist: List[str], send_email: bool = True):
    """
    Calls processor.process(...) to generate an email payload (and/or PDF),
    prints the returned body, then sends it via emailer.
    """
    print("\n=== [Processor] Building email payload ===")
    from src.processor.process import process  # your new processor entrypoint
    payload = process(textlist, jpblist)

    # Expect payload like: {"subject": str, "body": str, "attachments": [...]}
    body = payload.get("body", "")
    subject = payload.get("subject", "(no subject)")

    print("\n" + "=" * 70)
    print(f"EMAIL PREVIEW — {subject}")
    print("=" * 70)
    print(body)
    print("=" * 70 + "\n")

    if not send_email:
        print("[email-skip] send_email=False")
        return payload

    print("\n=== [Email] Sending ===")
    try:
        from src.utility.emailer import send_report
        send_report(payload)
        print("[email-ok] sent premarket report")
    except Exception as e:
        print(f"[email-error] {e}")
        traceback.print_exc()

    return payload


def _write_aggregate_pdf(filename: str, textlist: List[str], jpblist: List[str]) -> Path:
    out_dir = get_txt_dir()
    out_path = out_dir / filename

    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(str(out_path), pagesize=letter)
    width, height = letter
    x = 50
    y = height - 60

    c.setFont("Helvetica-Bold", 16)
    c.drawString(x, y, "Premarket Temporary Report")
    y -= 28
    c.setFont("Helvetica", 10)

    for t in textlist:
        path = Path(t)
        if not path.exists():
            continue
        c.setFont("Helvetica-Bold", 12)
        if y < 80:
            c.showPage()
            y = height - 60
            c.setFont("Helvetica", 10)
        c.drawString(x, y, f"===== FILE: {path.name} =====")
        y -= 16
        c.setFont("Helvetica", 10)
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if y < 60:
                c.showPage()
                y = height - 60
                c.setFont("Helvetica", 10)
            c.drawString(x, y, line[:140])
            y -= 13
        y -= 10

    for j in jpblist:
        path = Path(j)
        if not path.exists():
            continue
        c.showPage()
        y = height - 60
        c.setFont("Helvetica-Bold", 12)
        c.drawString(x, y, f"IMAGE: {path.name}")
        y -= 20
        try:
            c.drawImage(str(path), x, 80, width=width - 100, height=height - 160, preserveAspectRatio=True, anchor="sw")
        except Exception:
            c.setFont("Helvetica", 10)
            c.drawString(x, y, "[image failed to render]")
            y -= 13

    c.save()
    return out_path


def temporary_send(textlist: List[str], jpblist: List[str], send_email: bool = True):
    """
    Combine all text and image artifacts into one temporary PDF and send via email.
    """
    print("\n=== [Temporary] Building combined artifact ===")
    ts = _now_tag()
    pdf_path = _write_aggregate_pdf(f"temporary_report_{ts}.pdf", textlist, jpblist)
    _append_text(textlist, pdf_path)

    payload = {
        "subject": f"Premarket Temporary Report {ts}",
        "body": f"Temporary premarket report generated at {ts}. See attached PDF and image artifacts.",
        "attachments": [str(pdf_path)] + [str(Path(j)) for j in jpblist if Path(j).exists()],
    }

    if not send_email:
        print("[email-skip] send_email=False")
        return payload

    print("\n=== [Temporary] Sending email ===")
    try:
        from src.utility.emailer import send_report
        send_report(payload)
        print("[temporary-email-ok] sent temporary premarket report")
    except Exception as e:
        print(f"[temporary-email-error] {e}")
        traceback.print_exc()

    return payload


# ---------------------------
# Main
# ---------------------------

def main(send_email: bool = True, market_date=None):
    load_env()
    from src.utility.date import last_market_date

    market_date = market_date or last_market_date()
    print(f"[premarket] market_date={market_date}")

    # Artifact bundles:
    # - textlist: paths to txt/md/json outputs
    # - jpblist: paths to jpg/png heatmaps or other images
    textlist: List[str] = []
    jpblist: List[str] = []

    # 1) Reddit / ApeWisdom — MUST run first to update the dynamic ticker universe
    try:
        pipeline_reddit_wisdom(textlist)
    except Exception:
        print("\n[ERROR] ApeWisdom ticker-state pipeline failed.")
        traceback.print_exc()

    # Reload ticker universe from the freshly updated tickers.json
    from src.utility.constant import get_smf_tickers
    fresh_tickers = get_smf_tickers()
    from src.utility.constant import get_sectors
    sectors = get_sectors()
    print(f"[premarket] ticker universe size={len(fresh_tickers)}")

    # # 2) Equity pipeline (prices + flags)
    # # Comment this block out if you want to skip equity.
    # try:
    #     pipeline_equity(textlist, jpblist, market_date=market_date, tickers=fresh_tickers)
    # except Exception:
    #     print("\n[ERROR] Equity pipeline failed.")
    #     traceback.print_exc()

    # # # Pause between equity and options pipelines to avoid rate-limit on free tier
    # # print("\n=== [Rate-limit pause] Waiting 60s before options pipeline ===")
    # # for remaining in range(60, 0, -10):
    # #     print(f"  ...{remaining}s remaining")
    # #     time.sleep(10)
    # # print("  ...resuming")

    # # 3) Options pipeline (IV + iv-spike flags)
    # # Comment this block out if you want to skip options.
    # try:
    #     options = pipeline_options(textlist, jpblist, market_date=market_date, tickers=sectors)
    # except Exception:
    #     print("\n[ERROR] Options pipeline failed.")
    #     traceback.print_exc()

    # 4) News pipeline (GDELT fetch + top-20 clusters)
    # Comment this block out if you want to skip news.
    try:
        pipeline_news(textlist)
    except Exception:
        print("\n[ERROR] News pipeline failed.")
        traceback.print_exc()

    # # 5) Reddit → Mongo ingestion + sentiment
    # # Comment this block out if you want to skip reddit ingestion.
    # try:
    #     pipeline_reddit_mongo()
    # except Exception:
    #     print("\n[ERROR] Reddit mongo/sentiment pipeline failed.")
    #     traceback.print_exc()

    # # 6) Reddit heatmap + top mentions snapshot
    # # Comment this block out if you want to skip reddit plotting.
    # try:
    #     pipeline_reddit_graph_and_summary(textlist, jpblist)
    # except Exception:
    #     print("\n[ERROR] Reddit graph/snapshot pipeline failed.")
    #     traceback.print_exc()

    # # 7) Processor → email payload → send
    # # Comment out to stop at artifacts only.
    # try:
    #     process_and_send(textlist, jpblist, send_email=send_email)
    # except Exception:
    #     print("\n[ERROR] Processor/email step failed.")
    #     traceback.print_exc()

    # 8) Temporary send fallback
    # Use this instead of process_and_send if OpenAI is unavailable.
    # Uncomment the lines below to run temporary_send instead of process_and_send.
    try:
        temporary_send(textlist, jpblist, send_email=send_email)
    except Exception:
        print("\n[ERROR] Temporary send step failed.")
        traceback.print_exc()


if __name__ == "__main__":
    # For now, hard default to sending. You can add argparse later.
    main(send_email=False)