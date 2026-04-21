#!/usr/bin/env python3
"""
Email the latest PDF from data/morning_report/.

Cowork writes:  data/morning_report/{TIMESTAMP}_report.pdf
This script:    finds the latest one and emails it.

Usage:
  python jobs/send_report.py                          # auto-find latest PDF
  python jobs/send_report.py --pdf /path/to/file.pdf  # explicit path
  python jobs/send_report.py --subject "My Report"    # custom subject
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.env import load_env
from src.utility.constant import MORNING_REPORT_DIR


def find_latest_pdf(folder: Path) -> Path | None:
    pdfs = sorted(folder.glob("*_report.pdf"))
    return pdfs[-1] if pdfs else None


def main():
    parser = argparse.ArgumentParser(description="Email the latest morning report PDF")
    parser.add_argument("--pdf", type=str, default=None,
                        help="Explicit PDF path (default: latest in data/morning_report/)")
    parser.add_argument("--subject", type=str, default=None,
                        help="Email subject override")
    args = parser.parse_args()

    load_env()
    today = date.today().isoformat()

    if args.pdf:
        pdf_path = Path(args.pdf)
    else:
        report_dir = Path(MORNING_REPORT_DIR)
        pdf_path = find_latest_pdf(report_dir)
        if pdf_path is None:
            print(f"[send-report] no PDFs found in {report_dir}")
            sys.exit(1)

    if not pdf_path.exists():
        print(f"[send-report] PDF not found: {pdf_path}")
        sys.exit(1)

    print(f"[send-report] sending: {pdf_path.name}")

    from src.utility.emailer import send_report
    payload = {
        "subject": args.subject or f"Premarket Report from Jason — {today}",
        "body": f"Premarket report for {today}. See attached PDF.",
        "attachments": [str(pdf_path)],
    }
    send_report(payload)
    print("[send-report] done")


if __name__ == "__main__":
    main()
