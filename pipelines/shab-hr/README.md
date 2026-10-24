# shab-hr — company officers from the Swiss commercial-register gazette

Daily, after `amtsblattportal` has indexed the day's SHAB notices:

1. `fetch.py` — full text of new HR notices (GE + VD) from `https://www.amtsblattportal.ch/api/v1/publications/<id>/xml`
   into `bronze_ch.shab_hr_notice_texts`.
2. `load.py` — parses them (`parse.py`) into `silver_ch.shab_hr_company_events`, `shab_hr_person_roles`, `shab_hr_company_links`.
3. `links.py` — `silver_ch.refresh_officer_relationships()` (companies sharing a current officer) and
   `gold_ch.push_network_types(['shares_officer_with'])` to LaMap and LBI.

Current officers: `silver_ch.v_officers_current`. Backfill (2020→) was run once on 2026-10-24.
Rollback SQL and design notes: Command Center change_log, 2026-10-24.
