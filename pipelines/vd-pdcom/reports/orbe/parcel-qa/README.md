# Orbe official parcel sensitivity

Official Vaud layer21 provides1325 unique, current-commune, valid geometry reference objects in the source envelope. Count and paginated response receipts retained. Inner edge intersects1064objects (1036land +28DDP superficie), outer1098(1070land +28DDP);34objects depend on band choice. The band touches261objects but is not an additional sector.

Inner overlaps:0under1m²,3under1%parcel area. Outer:2under1m²,18under1%. Illustrative inner−10m/outer+10m scenarios yield1055–1155objects; not an accuracy interval. Land and rights can overlap; no exclusive-area sum.

All official intersection identities exist in silver_ch.cadastral_plots with Orbe attribution. Mirror geometry adds2inner/3outer intersections. Current official identity query does not return CH474509288365 or CH574509848357; mirror overlaps are59.10/59.455m². Cause unresolved, not proof of deletion. CH814597838340 is returned but current geometry has zero overlap, versus0.0354m² outer overlap in the mirror. Keep exact query and geometry evidence; do not silently include unmatched historical identities.

check.py acquires scoped official data and computes scenarios. verify.py reproduces all seven frozen keysets/areas. No runtime sectors/parcel pairs or receiver changes. Source exact-version approval, category fragments and positional caveats remain.

Next use official-reference snapshots for private candidate delivery. The envelope exceeds the helper’s2km axis limit: tile into bounded queries, reconcile overlapping EGRID geometry and preserve snapshot FK provenance. Preserve inner/outer membership and DDP distinctions; existing helper limits need not be widened.
