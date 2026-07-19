"""
rewrite_notebook.py
Thay the noi dung cac code cell cua Task 1 va Task 4 trong notebook
bang cac lenh run_wsl() goi external .py files.
"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"
CONDA = "/home/myha/miniconda3/bin/python3"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)


def set_code_cell(idx, lines):
    """Thay the noi dung code cell tai vi tri idx."""
    nb["cells"][idx]["source"] = lines
    nb["cells"][idx]["outputs"] = []
    nb["cells"][idx]["execution_count"] = None


# ── Task 1: cell 05 -> clone.py ──────────────────────────────────────────────
set_code_cell(5, [
    "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task1/clone.py\")\n",
    "print(r.stdout)\n",
    "if r.returncode != 0:\n",
    "    print('STDERR:', r.stderr[:500])",
])

# ── Task 1: cells 07 + 09 + 11 + 13 + 14 -> discover.py ─────────────────────
# Giu lai cell 07, xoa noi dung cells 09 11 13 14
set_code_cell(7, [
    "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task1/discover.py\")\n",
    "print(r.stdout)\n",
    "if r.returncode != 0:\n",
    "    print('STDERR:', r.stderr[:500])",
])

# Cell 09, 11, 13, 14 bien thanh cell ghi chu ngan
for idx in [9, 11, 13, 14]:
    set_code_cell(idx, [
        "# ket qua duoc in boi discover.py o tren\n",
        "# xem output/discovered_files.csv de xem data day du",
    ])

# ── Task 4: thay the 4 code cell (45, 47, 49, 51) ────────────────────────────
# Them bien CONDA_WSL vao cell 03 (imports) de dung chung
# Thay vi sua cell 03, ta dinh nghia lai trong moi cell Task 4

t4_cells = {
    45: [
        "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task4/start.py\", timeout=400)\n",
        "print(r.stdout)\n",
        "if r.returncode != 0:\n",
        "    print('STDERR:', r.stderr[:500])",
    ],
    47: [
        "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task4/setup.py\")\n",
        "print(r.stdout)\n",
        "if r.returncode != 0:\n",
        "    print('STDERR:', r.stderr[:500])",
    ],
    49: [
        "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task4/emit.py\", timeout=300)\n",
        "print(r.stdout)\n",
        "if r.returncode != 0:\n",
        "    print('STDERR:', r.stderr[:500])",
    ],
    51: [
        "r = run_wsl(f\"{CONDA_WSL} /mnt/e/BD/task4/verify.py\")\n",
        "print(r.stdout)\n",
        "if r.returncode != 0:\n",
        "    print('STDERR:', r.stderr[:500])",
    ],
}

for idx, lines in t4_cells.items():
    set_code_cell(idx, lines)

# ── Them CONDA_WSL vao cell 03 (imports & config) ────────────────────────────
# Tim dong cuoi cell 03 va them bien CONDA_WSL
cell03_src = nb["cells"][3]["source"]
# Kiem tra xem da co CONDA_WSL chua
if not any("CONDA_WSL" in line for line in cell03_src):
    # Them vao cuoi
    cell03_src.append("\n")
    cell03_src.append("# conda python trong WSL (co du pandas, kafka-python, neo4j)\n")
    cell03_src.append("CONDA_WSL = \"/home/myha/miniconda3/bin/python3\"\n")

# ── Luu ──────────────────────────────────────────────────────────────────────
with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Notebook da duoc cap nhat!")
print("Cell 05      : run clone.py")
print("Cell 07      : run discover.py")
print("Cell 09,11,13,14: placeholder")
print("Cell 45      : run start.py")
print("Cell 47      : run setup.py")
print("Cell 49      : run emit.py")
print("Cell 51      : run verify.py")
print("Cell 03      : them CONDA_WSL variable")
