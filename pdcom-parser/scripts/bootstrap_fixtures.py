"""Fetch the Chêne-Bougeries PDCom fixture PDF if not present."""
from __future__ import annotations

from pathlib import Path

URL = "https://www.chene-bougeries.ch/fileadmin/downloads/Vivre/Amenagement_territoire/PDCom/24_07_PDCom-Chene-Bougeries_version_approuvee_CE_17_04_2024.pdf"
DEST = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "chene_bougeries_pdcom_2024.pdf"


def main():
    if DEST.exists():
        print(f"Fixture already present at {DEST} ({DEST.stat().st_size // 1024} KB)")
        return
    from pdcom_parser.download import download_pdf
    print(f"Downloading fixture → {DEST}")
    download_pdf(URL, DEST)
    print(f"Done: {DEST} ({DEST.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
