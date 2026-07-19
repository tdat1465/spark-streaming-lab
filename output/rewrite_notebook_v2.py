"""
rewrite_notebook_v2.py
Rewrite cac cell Task 1 va Task 4 trong notebook:
- Task 1: import ham tu task1/ va goi ham
- Task 4: define wrapper function ngan + goi ham (vi phu thuoc WSL conda)
"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)


def set_code(idx, lines):
    nb["cells"][idx]["source"] = lines
    nb["cells"][idx]["outputs"] = []
    nb["cells"][idx]["execution_count"] = None


# ── Cell 03: them sys.path + import tu task1 ──────────────────────────────────
# Doc noi dung hien tai
src03 = list(nb["cells"][3]["source"])
src03_text = "".join(src03)

additions_03 = []
if "sys.path.insert" not in src03_text:
    additions_03 += [
        "\n",
        "# them project root vao path de import task1, task4 nhu package\n",
        "sys.path.insert(0, str(Path(os.path.abspath('.')).parent) "
        "if Path(os.path.abspath('.')).name == 'task1' else str(Path(os.path.abspath('.'))))\n",
    ]
if "from task1" not in src03_text:
    additions_03 += [
        "\n",
        "from task1.clone   import clone_repo_if_needed\n",
        "from task1.discover import (\n",
        "    is_auto_generated, classify_file, count_lines,\n",
        "    scan_repo, build_dataframe, smoke_test\n",
        ")\n",
    ]
if "CONDA_WSL" not in src03_text:
    additions_03 += [
        "\n",
        "# conda python trong WSL (dung cho task4 scripts)\n",
        "CONDA_WSL = '/home/myha/miniconda3/bin/python3'\n",
    ]
if "def run_wsl" not in src03_text:
    additions_03 += [
        "\n",
        "def run_wsl(cmd, timeout=120):\n",
        "    return subprocess.run(\n",
        "        ['wsl', '-e', 'bash', '-c', cmd],\n",
        "        capture_output=True, text=True,\n",
        "        encoding='utf-8', errors='replace', timeout=timeout\n",
        "    )\n",
    ]

if additions_03:
    nb["cells"][3]["source"] = src03 + additions_03
    print(f"Cell 03: them {len(additions_03)} dong")

# ── Cell 05: clone repo ───────────────────────────────────────────────────────
set_code(5, [
    "clone_repo_if_needed(REPO_ROOT, REPO_CLONE_URL)\n",
    "\n",
    "assert REPO_ROOT.exists(), f'[ERROR] Khong tim thay repo tai {REPO_ROOT}'\n",
    "print(f'[OK] Repo hop le: {REPO_ROOT}')",
])

# ── Cell 07: scan repo ────────────────────────────────────────────────────────
set_code(7, [
    "all_py_files_relative, categories = scan_repo(REPO_ROOT)\n",
    "\n",
    "print(f'\\nVi du 10 file dau:')\n",
    "for p in all_py_files_relative[:10]:\n",
    "    print(f'  {p}')",
])

# ── Cell 09: phan loai ────────────────────────────────────────────────────────
set_code(9, [
    "category_counts = Counter(categories)\n",
    "\n",
    "print('=' * 50)\n",
    "print('KET QUA PHAN LOAI FILE .py')\n",
    "print('=' * 50)\n",
    "for cat in ['source', 'test', 'setup', 'auto-generated']:\n",
    "    print(f'  {cat:<20}: {category_counts.get(cat, 0):>5} file(s)')\n",
    "print('-' * 50)\n",
    "print(f'  {\"TONG\":<20}: {sum(category_counts.values()):>5} file(s)')\n",
    "\n",
    "# kiem tra conftest.py phai la setup, khong phai test\n",
    "conftest = [(p, c) for p, c in zip(all_py_files_relative, categories)\n",
    "            if p.name == 'conftest.py']\n",
    "print('\\nKiem tra conftest.py (phai la setup):')\n",
    "for path, cat in conftest[:5]:\n",
    "    status = 'OK' if cat == 'setup' else 'LOI'\n",
    "    print(f'  [{status}] {path} -> {cat}')",
])

# ── Cell 11: smoke test ───────────────────────────────────────────────────────
set_code(11, [
    "smoke_test()",
])

# ── Cell 13: build DataFrame ──────────────────────────────────────────────────
set_code(13, [
    "df = build_dataframe(REPO_ROOT, all_py_files_relative, categories, OUTPUT_DIR)\n",
    "\n",
    "import pandas as pd\n",
    "pd.set_option('display.max_colwidth', 80)\n",
    "pd.set_option('display.width', 120)\n",
    "print('\\n10 dong dau:')\n",
    "df.head(10)",
])

# ── Cell 14: summary stats ────────────────────────────────────────────────────
set_code(14, [
    "summary_df = (\n",
    "    df.groupby('category')\n",
    "      .agg(\n",
    "          num_files   = ('relative_path', 'count'),\n",
    "          total_bytes = ('size_bytes',    'sum'),\n",
    "          total_lines = ('num_lines',     'sum'),\n",
    "          avg_lines   = ('num_lines',     'mean'),\n",
    "      )\n",
    "      .reset_index()\n",
    "      .sort_values('num_files', ascending=False)\n",
    ")\n",
    "summary_df['avg_lines'] = summary_df['avg_lines'].round(1)\n",
    "print('Thong ke theo category:')\n",
    "summary_df",
])

# ── Cell 16: lab report (doc lai df tu bien trong namespace) ──────────────────
set_code(16, [
    "# luc nay df va category_counts da co trong namespace tu cell 13 va 09\n",
    "total_py_files = len(df)\n",
    "n_test         = category_counts.get('test', 0)\n",
    "n_setup        = category_counts.get('setup', 0)\n",
    "n_autogen      = category_counts.get('auto-generated', 0)\n",
    "n_excluded     = n_test + n_setup + n_autogen\n",
    "n_source       = category_counts.get('source', 0)\n",
    "\n",
    "print('╔══════════════════════════════════════════════════════╗')\n",
    "print('║           TASK 1 – FINAL SUMMARY (Lab Report)        ║')\n",
    "print('╠══════════════════════════════════════════════════════╣')\n",
    "print(f'║  Repository : huggingface/peft                       ║')\n",
    "print(f'║  URL        : https://github.com/huggingface/peft    ║')\n",
    "print('╠══════════════════════════════════════════════════════╣')\n",
    "print(f'║  Total .py files discovered        : {total_py_files:>5}           ║')\n",
    "print('╠──────────────────────────────────────────────────────╣')\n",
    "print(f'║  Excluded – test files             : {n_test:>5}           ║')\n",
    "print(f'║  Excluded – setup/init files       : {n_setup:>5}           ║')\n",
    "print(f'║  Excluded – auto-generated files   : {n_autogen:>5}           ║')\n",
    "print(f'║  Total excluded                    : {n_excluded:>5}           ║')\n",
    "print('╠══════════════════════════════════════════════════════╣')\n",
    "print(f'║  SOURCE files -> CPG Parser Service : {n_source:>5}          ║')\n",
    "print('╚══════════════════════════════════════════════════════╝')",
])

# ── Task 4 cells: thin wrapper functions + goi ham ────────────────────────────
# Cell 45: start
set_code(45, [
    "def start_task4_services():\n",
    "    r = run_wsl(f'{CONDA_WSL} /mnt/e/BD/task4/start.py', timeout=400)\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "start_task4_services()\n",
])

# Cell 47: setup connector
set_code(47, [
    "def register_neo4j_connectors():\n",
    "    r = run_wsl(f'{CONDA_WSL} /mnt/e/BD/task4/setup.py')\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "register_neo4j_connectors()\n",
])

# Cell 49: emit
set_code(49, [
    "def emit_cpg_events():\n",
    "    r = run_wsl(f'{CONDA_WSL} /mnt/e/BD/task4/emit.py', timeout=300)\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "emit_cpg_events()\n",
])

# Cell 51: verify
set_code(51, [
    "def verify_neo4j_ingestion():\n",
    "    r = run_wsl(f'{CONDA_WSL} /mnt/e/BD/task4/verify.py')\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "verify_neo4j_ingestion()\n",
])

# ── Luu ──────────────────────────────────────────────────────────────────────
with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("\nNotebook da duoc cap nhat:")
print("  Cell 03 : import tu task1 + run_wsl + CONDA_WSL")
print("  Cell 05 : clone_repo_if_needed(REPO_ROOT, REPO_CLONE_URL)")
print("  Cell 07 : scan_repo(REPO_ROOT)")
print("  Cell 09 : Counter + phan loai")
print("  Cell 11 : smoke_test()")
print("  Cell 13 : build_dataframe(...)")
print("  Cell 14 : summary stats")
print("  Cell 16 : lab report (dung df tu namespace)")
print("  Cell 45 : start_task4_services()")
print("  Cell 47 : register_neo4j_connectors()")
print("  Cell 49 : emit_cpg_events()")
print("  Cell 51 : verify_neo4j_ingestion()")
