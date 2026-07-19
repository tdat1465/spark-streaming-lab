"""Them run_wsl va CONDA_WSL vao cell 03 cua notebook"""
import json

NOTEBOOK_PATH = "/mnt/e/BD/notebook.ipynb"

with open(NOTEBOOK_PATH, encoding="utf-8") as f:
    nb = json.load(f)

cell03 = nb["cells"][3]
src = "".join(cell03["source"])

# Them vao cuoi cell 03 neu chua co
additions = []

if "def run_wsl" not in src:
    additions += [
        "\n",
        "\n",
        "# ham tien ich: chay lenh bash trong WSL tu Windows\n",
        "def run_wsl(cmd, timeout=120):\n",
        "    return subprocess.run(\n",
        "        [\"wsl\", \"-e\", \"bash\", \"-c\", cmd],\n",
        "        capture_output=True, text=True,\n",
        "        encoding=\"utf-8\", errors=\"replace\",\n",
        "        timeout=timeout\n",
        "    )\n",
    ]

if "CONDA_WSL" not in src:
    additions += [
        "\n",
        "# conda python trong WSL (co du pandas, kafka-python, neo4j)\n",
        "CONDA_WSL = \"/home/myha/miniconda3/bin/python3\"\n",
    ]

if additions:
    cell03["source"] = list(cell03["source"]) + additions
    print(f"Da them {len(additions)} dong vao cell 03")
else:
    print("Cell 03 da co run_wsl va CONDA_WSL roi")

with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Xong! Hay chay lai Cell 03 trong notebook.")
