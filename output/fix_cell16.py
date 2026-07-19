"""Fix cell 16: doc df tu CSV thay vi dung bien tu cell truoc"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)

# Cell 16 la lab report, can them doc CSV
new_src = [
    "from collections import Counter\n",
    "\n",
    "# doc lai tu CSV (vi discover.py chay trong process rieng)\n",
    "df = pd.read_csv(OUTPUT_DIR / \"discovered_files.csv\")\n",
    "category_counts = Counter(df[\"category\"].tolist())\n",
    "\n",
    "# ── Cac con so chinh can ghi vao Lab Report ───────────────────────────────\n",
    "total_py_files = len(df)\n",
    "n_test         = category_counts.get(\"test\", 0)\n",
    "n_setup        = category_counts.get(\"setup\", 0)\n",
    "n_autogen      = category_counts.get(\"auto-generated\", 0)\n",
    "n_excluded     = n_test + n_setup + n_autogen\n",
    "n_source       = category_counts.get(\"source\", 0)\n",
    "\n",
    "print(\"╔══════════════════════════════════════════════════════╗\")\n",
    "print(\"║           TASK 1 – FINAL SUMMARY (Lab Report)        ║\")\n",
    "print(\"╠══════════════════════════════════════════════════════╣\")\n",
    "print(f\"║  Repository : huggingface/peft                       ║\")\n",
    "print(f\"║  URL        : https://github.com/huggingface/peft    ║\")\n",
    "print(\"╠══════════════════════════════════════════════════════╣\")\n",
    "print(f\"║  Total .py files discovered        : {total_py_files:>5}           ║\")\n",
    "print(\"╠──────────────────────────────────────────────────────╣\")\n",
    "print(f\"║  Excluded – test files             : {n_test:>5}           ║\")\n",
    "print(f\"║  Excluded – setup/init files       : {n_setup:>5}           ║\")\n",
    "print(f\"║  Excluded – auto-generated files   : {n_autogen:>5}           ║\")\n",
    "print(f\"║  Total excluded                    : {n_excluded:>5}           ║\")\n",
    "print(\"╠══════════════════════════════════════════════════════╣\")\n",
    "print(f\"║  SOURCE files -> CPG Parser Service : {n_source:>5}          ║\")\n",
    "print(\"╚══════════════════════════════════════════════════════╝\")\n",
    "\n",
    "# hien thi summary theo category\n",
    "print(\"\\nBreakdown:\")\n",
    "print(df.groupby('category').agg(num_files=('relative_path','count'), total_lines=('num_lines','sum')).to_string())\n",
]

nb["cells"][16]["source"] = new_src
nb["cells"][16]["outputs"] = []
nb["cells"][16]["execution_count"] = None

with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Da sua cell 16 - doc df tu CSV.")
