"""
fix_notebook_paths.py
Cap nhat cell 03 va cells Task 4 trong notebook:
- Them ham to_wsl_path() va CONDA_WSL dong (detect tu WSL)
- Thay the duong dan hardcode /mnt/e/BD bang bien dong
"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)


def set_code(idx, lines):
    nb["cells"][idx]["source"] = lines
    nb["cells"][idx]["outputs"] = []
    nb["cells"][idx]["execution_count"] = None


# ── Cell 03: them to_wsl_path + CONDA_WSL dong ────────────────────────────────
src03 = list(nb["cells"][3]["source"])
src03_text = "".join(src03)

# Xoa dong CONDA_WSL cu (hardcode) neu co
src03_filtered = [
    line for line in src03
    if "CONDA_WSL" not in line or "def " in line or "#" in line.strip()[:2]
]

# Xoa ham run_wsl cu neu co (se them lai ben duoi)
filtered2 = []
skip = False
for line in src03_filtered:
    if "def run_wsl" in line:
        skip = True
    if skip and line.strip() == "":
        skip = False
        continue
    if not skip:
        filtered2.append(line)

additions = []

if "to_wsl_path" not in src03_text:
    additions += [
        "\n",
        "\n",
        "# chuyen duong dan Windows sang WSL: e:\\BD -> /mnt/e/BD\n",
        "def to_wsl_path(win_path):\n",
        "    p = str(win_path).replace('\\\\', '/')\n",
        "    if len(p) >= 2 and p[1] == ':':\n",
        "        drive, rest = p[0].lower(), p[2:]\n",
        "        return f'/mnt/{drive}{rest}'\n",
        "    return p\n",
    ]

if "WSL_ROOT" not in src03_text:
    additions += [
        "\n",
        "WSL_ROOT  = to_wsl_path(PROJECT_ROOT)   # /mnt/e/BD\n",
        "TASK4_WSL = f'{WSL_ROOT}/task4'          # /mnt/e/BD/task4\n",
    ]

if "CONDA_WSL" not in "".join(additions) + src03_text:
    additions += [
        "\n",
        "# tim conda python trong WSL (khong set cung username)\n",
        "def find_conda_wsl():\n",
        "    import subprocess as _sp\n",
        "    for d in ['miniconda3', 'anaconda3', 'miniforge3']:\n",
        "        r = _sp.run(['wsl', '-e', 'bash', '-c', f'ls ~/{{d}}/bin/python3'],\n",
        "                    capture_output=True, text=True)\n",
        "        if r.returncode == 0:\n",
        "            return r.stdout.strip()\n",
        "    return 'python3'\n",
        "\n",
        "CONDA_WSL = find_conda_wsl()\n",
    ]

if "def run_wsl" not in "".join(additions) + src03_text:
    additions += [
        "\n",
        "def run_wsl(cmd, timeout=120):\n",
        "    return subprocess.run(\n",
        "        ['wsl', '-e', 'bash', '-c', cmd],\n",
        "        capture_output=True, text=True,\n",
        "        encoding='utf-8', errors='replace', timeout=timeout\n",
        "    )\n",
    ]

nb["cells"][3]["source"] = filtered2 + additions
print(f"Cell 03: cap nhat xong")

# ── Task 4 cells: dung bien TASK4_WSL thay vi hardcode ────────────────────────
set_code(45, [
    "def start_task4_services():\n",
    "    r = run_wsl(f'{CONDA_WSL} {TASK4_WSL}/start.py', timeout=400)\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "start_task4_services()\n",
])

set_code(47, [
    "def register_neo4j_connectors():\n",
    "    r = run_wsl(f'{CONDA_WSL} {TASK4_WSL}/setup.py')\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "register_neo4j_connectors()\n",
])

set_code(49, [
    "def emit_cpg_events():\n",
    "    r = run_wsl(f'{CONDA_WSL} {TASK4_WSL}/emit.py', timeout=300)\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "emit_cpg_events()\n",
])

set_code(51, [
    "def verify_neo4j_ingestion():\n",
    "    r = run_wsl(f'{CONDA_WSL} {TASK4_WSL}/verify.py')\n",
    "    if r.stdout: print(r.stdout)\n",
    "    if r.returncode != 0: print('STDERR:', r.stderr[:500])\n",
    "\n",
    "verify_neo4j_ingestion()\n",
])

print("Cell 45,47,49,51: dung TASK4_WSL thay hardcode")

# ── Luu ──────────────────────────────────────────────────────────────────────
with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Xong!")
