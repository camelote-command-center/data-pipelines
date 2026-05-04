"""
T3 — Rest of CH, quarterly (cron 15th of month).
Build: AG, LU, SO, OW, NW, AI. Refresh: SG, GR, TG, SZ, SH, AR, GL, ZG, UR.
Writes to re-LLM bronze_ch.federal_cadastral_parcels via the shared engine.
GE excluded — handled by ge_cad_parcelles separately.
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.cadastral_common import TierConfig, run_tier

if __name__ == "__main__":
    sys.exit(run_tier(TierConfig(
        tier_label="t3",
        cantons=[
            "AG", "LU", "SO", "OW", "NW", "AI",     # build (currently 0 rows)
            "SG", "GR", "TG", "SZ", "SH", "AR",     # refresh
            "GL", "ZG", "UR",                       # refresh
        ],
        dataset_code="federal_cadastral_parcels_t3",
    )))
