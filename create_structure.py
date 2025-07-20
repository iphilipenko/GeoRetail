import os
from pathlib import Path

# Структура проекту
project_structure = [
    "config",
    "src",
    "src/data",
    "src/graph", 
    "src/embeddings",
    "src/models",
    "tests",
    "scripts",
    "notebooks",
    "data",
    "data/raw",
    "data/processed", 
    "data/models"
]

# Створюємо папки
for folder in project_structure:
    Path(folder).mkdir(parents=True, exist_ok=True)
    print(f"✅ Створено: {folder}")

# Створюємо __init__.py файли
init_files = [
    "config/__init__.py",
    "src/__init__.py",
    "src/data/__init__.py",
    "src/graph/__init__.py",
    "src/embeddings/__init__.py",
    "src/models/__init__.py",
    "tests/__init__.py"
]

for init_file in init_files:
    Path(init_file).touch()
    print(f"✅ Створено: {init_file}")

# Створюємо .gitkeep файли
gitkeep_files = [
    "data/raw/.gitkeep",
    "data/processed/.gitkeep", 
    "data/models/.gitkeep"
]

for gitkeep in gitkeep_files:
    Path(gitkeep).touch()
    print(f"✅ Створено: {gitkeep}")

print("\n🎉 Структура проекту створена!")