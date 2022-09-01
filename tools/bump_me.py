import shutil
from pathlib import Path

import argparse
import sys

"""
This script is used to add to the changelog on release
"""
def parse_args():
    parser = argparse.ArgumentParser(description="convert flaky result")
    parser.add_argument('version')
    parser.add_argument('changes', nargs="?", type=argparse.FileType('r', encoding='UTF-8'), default=sys.stdin)
    return parser.parse_args()


def main():
    args = parse_args()

    this_path = Path(__file__).absolute()
    docs_dir = this_path.parent.parent / "docs"
    cl = docs_dir / "changelog.rst"
    cl_new = docs_dir / "changelog.rst.tmp"
    cl_new.touch()

    with cl.open("r") as cl_file, cl_new.open("w") as cl_new_file:
        cl_file_iter = iter(cl_file)
        for line in cl_file_iter:
            cl_new_file.write(line)
            if line == "Changelog\n":
                cl_new_file.write(next(cl_file_iter)) # skip =====
                cl_new_file.write(next(cl_file_iter)) # skip \n
                # do the append
                print(args.version, file=cl_new_file)
                print("=" * len(args.version), file=cl_new_file)
                for line in args.changes:
                    cl_new_file.write(line)
                print("", file=cl_new_file)

    shutil.copy(cl_new, cl)
    cl_new.unlink()


if __name__ == '__main__':
    main()
