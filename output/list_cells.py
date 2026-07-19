import json

with open("/mnt/e/BD/notebook.ipynb") as f:
    nb = json.load(f)

cells = nb["cells"]
print(f"Total cells: {len(cells)}")
for i, c in enumerate(cells):
    src = "".join(c["source"])[:120].replace("\n", " ")
    print(f"[{i:02d}] {c['cell_type']:8s} | {src}")
