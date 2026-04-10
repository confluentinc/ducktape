import os
import tarfile
import tempfile

import pytest

from ducktape.utils.compress import compress_test_results_dir


class CheckCompress(object):
    def check_compress_creates_tgz_and_removes_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "TestClass", "test_method", "1")
            os.makedirs(test_dir)

            with open(os.path.join(test_dir, "test_log.info"), "w") as f:
                f.write("some log content\n")
            with open(os.path.join(test_dir, "report.json"), "w") as f:
                f.write('{"test_status": "PASS"}\n')

            tgz_path = compress_test_results_dir(test_dir)

            assert tgz_path == test_dir + ".tgz"
            assert os.path.isfile(tgz_path)
            assert not os.path.exists(test_dir)

            with tarfile.open(tgz_path, "r:gz") as tar:
                names = tar.getnames()
                assert "1/test_log.info" in names
                assert "1/report.json" in names

    def check_compress_nonexistent_dir_raises(self):
        with pytest.raises(FileNotFoundError):
            compress_test_results_dir("/tmp/nonexistent_dir_abc123")

    def check_compress_with_nested_service_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "1")
            os.makedirs(test_dir)

            service_dir = os.path.join(test_dir, "kafka", "node0")
            os.makedirs(service_dir)
            with open(os.path.join(test_dir, "test_log.info"), "w") as f:
                f.write("test log\n")
            with open(os.path.join(service_dir, "server.log"), "w") as f:
                f.write("kafka log content\n" * 100)

            tgz_path = compress_test_results_dir(test_dir)

            assert os.path.isfile(tgz_path)
            assert not os.path.exists(test_dir)

            with tarfile.open(tgz_path, "r:gz") as tar:
                names = tar.getnames()
                assert any("server.log" in n for n in names)

    def check_compress_returns_correct_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "my_test_dir")
            os.makedirs(test_dir)
            with open(os.path.join(test_dir, "test_log.info"), "w") as f:
                f.write("content\n")

            tgz_path = compress_test_results_dir(test_dir)

            assert tgz_path.endswith(".tgz")
            assert os.path.basename(tgz_path) == "my_test_dir.tgz"
