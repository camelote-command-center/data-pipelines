# Grandson selected-paint reconciliation

Resolved paint ordering among the four Bellevue/Borné-Nau hatch-support paths (0, 1, 64, 97). Each earlier path is reduced by later selected paths, preserving the total union without double counting. The source PDF and native-contour hashes are recorded. No snapping, buffering or geometry repair is used.

At 128 subdivisions per cubic, the raw sum is 53,611.8205 square PDF points and the union is 43,225.1423; 10,386.6782 square PDF points were double counted. Path areas after partition are 6,550.3928 (orange 0), 10,378.1743 (blue 1), 9,352.8409 (orange 64), and 16,943.7343 (yellow 97). These units are **not square metres**. The blue/orange overlap dominates, with minor seams also resolved in paint order.

Sampling sensitivity at 16/32/64/128 subdivisions is retained. The union changes by 0.008264 square PDF points from 64 to 128; this is a numerical convergence check, not cartographic accuracy. All partitions are valid; pairwise overlap and union difference are below 1e-8 square PDF points. Exact JSON replay passed and the partition image was visually checked against the prior source overlay.

This resolves only the four selected colored supports. Later green or solid fills, roads, buildings, alternative-use circles and other map symbols are not subtracted. The output is neither net development capacity nor current zoning. Geographic controls/alignment, complete thematic interpretation, currentness and exact approval remain pending. No runtime sector or parcel delivery changes.
