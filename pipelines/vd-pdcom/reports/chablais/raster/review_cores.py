"""Render evidence crops from the exact PDF and generated masks."""
import argparse,hashlib
from pathlib import Path
import numpy as np
import pymupdf as fitz
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from extract import SHA
p=argparse.ArgumentParser();p.add_argument('--pdf',type=Path,required=True);p.add_argument('--input',type=Path,required=True);p.add_argument('--cores',type=Path,required=True);a=p.parse_args()
if hashlib.sha256(a.pdf.read_bytes()).hexdigest()!=SHA:raise ValueError('source_sha_mismatch')
with fitz.open(a.pdf) as doc:
 pix=fitz.Pixmap(doc,doc[28].get_images()[0][0]);rgb=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,3)
raw=np.load(a.input/'masks.npz')['t35'];core=np.load(a.cores/'core-supported-masks.npz')['t35_depth3']
if raw.shape!=rgb.shape[:2] or core.shape!=raw.shape or np.any(core&~raw):raise ValueError('mask_mismatch')
fig,axes=plt.subplots(1,4,figsize=(14,7))
for ax,(name,box) in zip(axes,[('Aigle',[840,230,1000,480]),('Ollon',[1300,740,1450,860]),('Bex',[1450,1680,1600,1820]),('Dashed comparison (Valais)',[680,1430,790,1560])]):
 x0,y0,x1,y1=box;ax.imshow(rgb[y0:y1,x0:x1],extent=[x0,x1,y1,y0]);over=np.zeros((*raw[y0:y1,x0:x1].shape,4));over[core[y0:y1,x0:x1]]=[0,1,1,.8];over[(raw&~core)[y0:y1,x0:x1]]=[1,1,0,.9];ax.imshow(over,extent=[x0,x1,y1,y0]);ax.set_title(name);ax.set_xlabel('Source pixels')
fig.suptitle('Cyan: colour components with ≥3 px interior depth; yellow: remaining thin fragments\nVisual locality labels only — no geographic assignment or validated sector outlines');fig.tight_layout();fig.savefig(a.cores/'locality-review.png',dpi=150);plt.close(fig)
