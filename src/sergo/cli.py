import argparse
from pathlib import Path
import shutil

SERGO_TEMPLATE_DIR = Path(__file__).parent / "project_template"


def create_project(name, directory):
    target = Path(directory)
    if name:
        target /= name
    target.mkdir(parents=True, exist_ok=True)

    for src_path in SERGO_TEMPLATE_DIR.rglob('*'):
        if '__pycache__' in src_path.parts:
            continue
        dest_path = target / src_path.relative_to(SERGO_TEMPLATE_DIR)

        if src_path.is_dir():
            dest_path.mkdir(parents=True, exist_ok=True)
        elif src_path.is_file() and not dest_path.exists():
            shutil.copy2(src_path, dest_path)
            if name:
                dest_path.write_text(dest_path.read_text().replace('{{ project_name }}', name))
    print(f"Sergo project created")


def main():
    parser = argparse.ArgumentParser(description="Sergo admin commands")
    parser.add_argument('command', help="Command to run")
    parser.add_argument('name', nargs='?', default='', help="Optional name of the new Sergo project")
    parser.add_argument('--directory', default='.', help="Directory to create the project in")
    args = parser.parse_args()
    create_project(args.name, args.directory)


if __name__ == "__main__":
    main()
