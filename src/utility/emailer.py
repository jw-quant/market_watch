try:
    from . import constant as cfg
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import src.utility.constant as cfg

import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _make_plaintext(summary: pd.DataFrame, vol_spike: List[str], recent_abn: List[str]) -> str:
    lines = []
    lines.append("SMF Daily Summary")
    lines.append("")
    asof = summary["asof"].max() if (summary is not None and not summary.empty and "asof" in summary.columns) else "N/A"
    lines.append(f"As of: {asof}")
    lines.append("")
    lines.append("Vol spike: " + (", ".join(vol_spike) if vol_spike else "(none)"))
    lines.append("Recent abnormal: " + (", ".join(recent_abn) if recent_abn else "(none)"))
    lines.append("")

    if summary is not None and not summary.empty:
        cols = [c for c in ["symbol", "flag_vol_spike", "flag_recent_abnormal"] if c in summary.columns]
        if cols:
            tail = summary[cols].tail(20)
            lines.append(tail.to_string(index=False))

    return "\n".join(lines)


def _attach_csv(msg: EmailMessage, summary: pd.DataFrame, filename: str = "summary.csv") -> None:
    if summary is None or summary.empty:
        return
    csv_bytes = summary.to_csv(index=False).encode("utf-8")
    msg.add_attachment(csv_bytes, maintype="text", subtype="csv", filename=filename)


def send_report(payload):
    """
    New behavior:
      payload = {"subject": str, "body": str, "attachments": [path1, path2, ...]}
    This keeps your email pipeline simple: send_report(email_payload)
    """
    if isinstance(payload, dict) and "body" in payload:
        subject = payload.get("subject", cfg.EMAIL_SUBJECT)
        body = payload.get("body", "")
        attachments = payload.get("attachments", [])
        return send_payload(subject=subject, body=body, attachments=attachments)

    raise TypeError("send_report(payload) expects a dict with keys: subject, body, attachments")


def send_payload(subject: str, body: str, attachments: List[str] | None = None) -> None:
    if not cfg.EMAIL_TO:
        print("[email-skip] EMAIL_TO not configured; skipping send.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.EMAIL_FROM or cfg.SMTP_USER
    msg["To"] = ", ".join(cfg.EMAIL_TO)
    msg.set_content(body)

    attachments = attachments or []
    for p in attachments:
        try:
            path = Path(p)
            if not path.exists():
                continue
            data = path.read_bytes()
            # naive mime based on extension
            ext = path.suffix.lower()
            if ext == ".pdf":
                msg.add_attachment(data, maintype="application", subtype="pdf", filename=path.name)
            elif ext in [".jpg", ".jpeg"]:
                msg.add_attachment(data, maintype="image", subtype="jpeg", filename=path.name)
            elif ext == ".png":
                msg.add_attachment(data, maintype="image", subtype="png", filename=path.name)
            else:
                msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=path.name)
        except Exception as e:
            print(f"[email-warn] failed attaching {p}: {e}")

    # Send via SMTP (reuse your existing logic)
    try:
        if cfg.SMTP_PORT == 587:
            server = smtplib.SMTP(cfg.SMTP_HOST, cfg.SMTP_PORT, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
            if cfg.SMTP_USER and cfg.SMTP_PASS:
                server.login(cfg.SMTP_USER, cfg.SMTP_PASS)
            server.send_message(msg)
            server.quit()
        else:
            server = smtplib.SMTP_SSL(cfg.SMTP_HOST, cfg.SMTP_PORT, timeout=30)
            if cfg.SMTP_USER and cfg.SMTP_PASS:
                server.login(cfg.SMTP_USER, cfg.SMTP_PASS)
            server.send_message(msg)
            server.quit()
        print(f"[email-ok] sent to: {', '.join(cfg.EMAIL_TO)}")
    except Exception as e:
        print(f"[email-err] {e}")
        raise


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from src.common.env import load_env
    load_env()

    print(f"SMTP host  : {cfg.SMTP_HOST}:{cfg.SMTP_PORT}")
    print(f"from       : {cfg.EMAIL_FROM or cfg.SMTP_USER}")
    print(f"to         : {cfg.EMAIL_TO}")

    send_payload(
        subject="market_watch — email test",
        body="If you're reading this, SMTP is configured correctly.",
        attachments=[],
    )