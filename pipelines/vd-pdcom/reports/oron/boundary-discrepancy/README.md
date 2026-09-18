# Palezieux-Gare boundary discrepancy

Frozen provisional geometry minus the current registered Oron boundary produces one outside component: 686.781 m² (0.6168%), maximum sampled vertex distance 6.989 m. This is a narrow southwest strip, not a disconnected proposed sector. Full geometry and source-PDF coordinates are preserved in diagnostic.json.

Source and overlay images were visually compared: blue current boundary, magenta manual trace, orange outside component. The eastern curved hatch edge also reveals coarse manual tracing. These observations prevent treating the current polygon or footprint counts as precise allocations. No fit, trace, runtime geometry or boundary was changed; no silent clipping. Source intent, tracing and registration remain unresolved.

Next: refine the source-space hatch trace independently of the official boundary, preserve the original, then reassess the discrepancy and local ground controls. Consultation/currentness restrictions remain. diagnose.py reproduces diagnostics/images without overwriting human review.json; requires the original PDF at recovered-source/report.pdf.
