from pathlib import Path, PureWindowsPath

# Test 1: Path với backslash trên Linux
p1 = Path("docs\\source\\_config.py")
print("=== Test Path với backslash trên Linux ===")
print(f"  Path parts : {p1.parts}")
print(f"  File tồn tại: {(Path('/mnt/e/BD/peft') / p1).exists()}")

# Test 2: Dùng PureWindowsPath để convert đúng
p2 = Path(PureWindowsPath("docs\\source\\_config.py").as_posix())
print("\n=== Test PureWindowsPath → forward slash ===")
print(f"  Path parts : {p2.parts}")
print(f"  File tồn tại: {(Path('/mnt/e/BD/peft') / p2).exists()}")

# Test 3: replace thủ công
p3_str = "docs\\source\\_config.py".replace("\\", "/")
p3 = Path(p3_str)
print("\n=== Test replace backslash ===")
print(f"  Path parts : {p3.parts}")
print(f"  File tồn tại: {(Path('/mnt/e/BD/peft') / p3).exists()}")
