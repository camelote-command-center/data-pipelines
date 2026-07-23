import json
s=json.load(open("sizes.json"))
tot=sum(v["bytes"] for v in s.values())
# measured on tile 2502-1117: 23,737,414 pts in 150.0 MB on disk
BYTES_PER_PT = 157286400/23737414
n_est = tot/BYTES_PER_PT
print(f"307 tiles, {tot/1e9:.2f} GB compressed COPC")
print(f"measured on 2502-1117: {BYTES_PER_PT:.2f} bytes/point compressed")
print(f"=> canton point count ~ {n_est/1e9:.2f} BILLION points\n")
print("pnts payload per point, uncompressed:")
for label,b in [("float32 XYZ + RGB + HAG f32 + class u8", 3*4+3+4+1),
                ("quantized u16 XYZ + RGB + HAG f32 + class u8", 3*2+3+4+1),
                ("quantized u16 XYZ + RGB + HAG u16 + class u8", 3*2+3+2+1)]:
    print(f"  {label:46s} {b:2d} B  -> {n_est*b/1e9:7.1f} GB at full density")
print("\nwith uniform thinning of the FULL-RES leaf level:")
for th in (1,2,4,8,16):
    b=3*2+3+2+1
    print(f"  1/{th:<3d} ({23.7/th:5.1f} pts/m2)  leaves {n_est/th/1e9:5.2f} B pts -> {n_est/th*b/1e9:6.1f} GB (+ ~33% for the LOD pyramid = {n_est/th*b*1.33/1e9:6.1f} GB)")
