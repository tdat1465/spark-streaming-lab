"""Kiem tra va sua cell 03 trong notebook"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)

src = "".join(nb["cells"][3]["source"])
print("=== PHAN CUOI CELL 03 ===")
print(src[-800:])
