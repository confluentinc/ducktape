import shutil
from pathlib import Path
from tempfile import TemporaryFile, NamedTemporaryFile

if __name__ == '__main__':
    to_append = """- test1
    - test 2"""
    cl = Path("./docs/changelog.rst")
    cl_new = Path("./docs/changelog.rst.tmp")
    cl_new.touch()
    with cl.open("r") as cl_file, cl_new.open("w") as cl_new_file:
        cl_file_iter = iter(cl_file)
        for line in cl_file_iter:
            cl_new_file.write(line)
            if line == "Changelog\n":
                cl_new_file.write(next(cl_file_iter))
                # do the append
                print("", file=cl_new_file)
                print(to_append, file=cl_new_file)

    shutil.copy(cl_new, cl)
    cl_new.unlink()
