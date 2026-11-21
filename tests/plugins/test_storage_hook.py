"""Unit tests for Object Storage Hook and Storage Operators."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch
from plugins.hooks.storage_hook import StorageHook
from plugins.operators.storage_operators import UploadToStorageOperator, DownloadFromStorageOperator


class TestStorageHook(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="test_storage_")
        self.sample_file = os.path.join(self.temp_dir, "test_file.txt")
        with open(self.sample_file, "w") as f:
            f.write("sample content for storage test")

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        simulated_staging = os.path.join(os.getcwd(), "staging", "storage_remote")
        if os.path.exists(simulated_staging):
            shutil.rmtree(simulated_staging)

    def test_upload_and_download_fallback(self):
        hook = StorageHook("storage_default")
        with patch("boto3.client", side_effect=OSError("S3 unavailable")):
            dest = hook.upload_file(
                self.sample_file, "data/test_file.txt", bucket="test-bucket"
            )
            self.assertTrue(os.path.exists(dest))

            download_target = os.path.join(self.temp_dir, "downloaded.txt")
            hook.download_file(
                "data/test_file.txt", download_target, bucket="test-bucket"
            )
        self.assertTrue(os.path.exists(download_target))

        with open(download_target, "r") as f:
            self.assertEqual(f.read(), "sample content for storage test")

    def test_storage_operators(self):
        op_upload = UploadToStorageOperator(
            task_id="upload_test",
            local_files=[self.sample_file],
            remote_prefix="batch_raw",
            bucket="test-bucket",
        )
        res_upload = op_upload.execute({})
        self.assertEqual(len(res_upload), 1)

        download_dir = os.path.join(self.temp_dir, "op_download")
        op_download = DownloadFromStorageOperator(
            task_id="download_test",
            remote_keys=["batch_raw/test_file.txt"],
            local_dir=download_dir,
            bucket="test-bucket",
        )
        res_download = op_download.execute({})
        self.assertEqual(len(res_download), 1)
        self.assertTrue(os.path.exists(res_download[0]))


if __name__ == "__main__":
    unittest.main()
