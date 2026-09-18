# Recover municipal document routes

The crawler classified /_doc/NNN downloads as landing pages, so candidate extraction skipped them. It now marks those links as PDF candidates and the extractor also recognizes previously saved /_doc routes and the observed /Nnnn/plan-directeur-communal.html document route. Every response still passes PDF magic/network/size/time guards.

Live acquisition: Puidoux74pages, Allaman99pages, Commugny122pages; all3registered/read back,295pages total. Puidoux scanned cover visually shows signed council10May2001/canton18November2002 approval; OCR/currentness remain pending. Corpus96PDFs/40source communes; no geographic delivery increment. Lovatens downloaded3page document rejected by exact SHA match to already-reviewed permit guide. Jouxtens route failed; remains explicit exception.

Earlier deeper run35329958056 processed60communes with no new candidates, succeeded. Depth coverage160/300. IDs/hashes in registered.json; rawPDF/text local RE-LLM artifacts/vd-pdcom/document-routes. Keep old statuses/evidence; no completion promotion.
