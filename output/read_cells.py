"""In noi dung day du cua cell 03, 05, 07, 09"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"
with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)

for i in [3, 5, 7, 9]:
    c = nb["cells"][i]
    print(f"\n{'='*60}")
    print(f"CELL [{i:02d}] type={c['cell_type']}")
    print('='*60)
    print("".join(c["source"]))
