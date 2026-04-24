from __future__ import annotations

import fitz


def classify_page(page: fitz.Page) -> dict:
    drawings = page.get_drawings()
    text = page.get_text()
    txt_lower = text.lower()

    info = {
        "type": "unknown",
        "drawing_count": len(drawings),
        "total_text_chars": len(text),
        "has_raster": False,
        "page_number": page.number + 1,
    }

    try:
        images = page.get_images(full=False)
        info["has_raster"] = len(images) > 0
    except Exception:
        pass

    if "sommaire" in txt_lower or "table des matières" in txt_lower or "table des matieres" in txt_lower:
        info["type"] = "toc"
        return info

    # Real thematic maps typically have many drawings + minimal text (only legend labels + title).
    # "Programme de mise en œuvre" tables also have many drawings (colored cells) but with
    # much more text. Tighten the map heuristic to rule those out.
    if info["drawing_count"] >= 500 and info["total_text_chars"] < 1500:
        info["type"] = "map"
    elif info["drawing_count"] < 100 and info["total_text_chars"] > 500:
        info["type"] = "text"
    elif info["total_text_chars"] > 1500:
        info["type"] = "text"   # programme/table page
    elif page.number == 0:
        info["type"] = "cover"

    return info
