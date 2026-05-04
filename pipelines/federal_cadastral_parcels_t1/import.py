"""
T1 — Suisse Romande, monthly.
Build: VD, NE, JU. Refresh: FR, VS.
Writes to re-LLM bronze_ch.federal_cadastral_parcels via the shared engine.
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.cadastral_common import TierConfig, run_tier

if __name__ == "__main__":
    sys.exit(run_tier(TierConfig(
        tier_label="t1",
        cantons=["VD", "NE", "JU", "FR", "VS"],
        dataset_code="federal_cadastral_parcels_t1",
    )))
