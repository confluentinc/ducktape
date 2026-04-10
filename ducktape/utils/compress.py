import os
import shlex
import shutil
import subprocess


def compress_test_results_dir(results_dir):
    """Compress a test results directory to a .tgz archive and remove the original.
    Produces one .tgz per test directory, placed as a sibling to the original directory.

    :param results_dir: Absolute path to the test results directory (no trailing separator)
    :returns: Path to the resulting .tgz file
    :raises FileNotFoundError: If results_dir does not exist
    :raises subprocess.CalledProcessError: If tar fails
    """
    if not os.path.isdir(results_dir):
        raise FileNotFoundError(f"Results directory does not exist: {results_dir}")

    tar_working_dir = os.path.dirname(results_dir)
    tar_relative_path = os.path.basename(results_dir)
    tgz_path = results_dir + ".tgz"

    try:
        subprocess.check_output(
            "tar czf %s -C %s %s" % (
                shlex.quote(tgz_path), shlex.quote(tar_working_dir), shlex.quote(tar_relative_path)),
            shell=True,
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError:
        if os.path.exists(tgz_path):
            os.remove(tgz_path)
        raise

    shutil.rmtree(results_dir)

    return tgz_path
