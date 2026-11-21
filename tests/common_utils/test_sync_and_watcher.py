"""Unit tests for DAG Manifest Sync and Watcher tools."""

import os
import shutil
import tempfile
import time
import unittest

from tools.sync import (
    read_manifest,
    sync_dag,
    get_enabled_dags,
    save_enabled_dags,
    disable_dag,
    clean_target,
)
from tools.watcher import (
    DagWatcher,
    find_all_manifest_dags,
    get_files_snapshot,
)


class TestSyncAndWatcher(unittest.TestCase):
    def setUp(self):
        self.test_target = tempfile.mkdtemp(prefix="test_enabled_dags_")
        self.sample_dag = "dags/finance/invoicing/partner_billing_etl"

    def tearDown(self):
        if os.path.exists(self.test_target):
            shutil.rmtree(self.test_target)

    def test_read_manifest(self):
        manifest_path = os.path.join(self.sample_dag, "manifest")
        deps = read_manifest(manifest_path)
        self.assertIn("dags/finance/invoicing/partner_billing_etl", deps)
        self.assertIn("dags/common_utils", deps)
        self.assertIn("plugins/hooks", deps)

    def test_sync_dag_and_state(self):
        deps = sync_dag(self.sample_dag, target_dir=self.test_target, quiet=True)
        self.assertTrue(len(deps) >= 3)

        # Check target contains synced DAG
        target_dag_file = os.path.join(
            self.test_target,
            "dags/finance/invoicing/partner_billing_etl/dag.py",
        )
        self.assertTrue(os.path.exists(target_dag_file))

        # Check common_utils is synced
        target_utils_file = os.path.join(
            self.test_target,
            "dags/common_utils/airflow.py",
        )
        self.assertTrue(os.path.exists(target_utils_file))

        # Verify state file
        enabled = get_enabled_dags(self.test_target)
        self.assertIn(self.sample_dag, enabled)

    def test_disable_dag(self):
        sync_dag(self.sample_dag, target_dir=self.test_target, quiet=True)
        self.assertIn(self.sample_dag, get_enabled_dags(self.test_target))

        disable_dag(self.sample_dag, target_dir=self.test_target)
        self.assertNotIn(self.sample_dag, get_enabled_dags(self.test_target))

        target_dag_dir = os.path.join(self.test_target, self.sample_dag)
        self.assertFalse(os.path.exists(target_dag_dir))

    def test_find_all_manifest_dags(self):
        manifest_dags = find_all_manifest_dags("dags")
        self.assertTrue(len(manifest_dags) >= 2)
        self.assertIn("dags/finance/invoicing/partner_billing_etl", manifest_dags)

    def test_watcher_snapshot_and_affected_dags(self):
        watcher = DagWatcher(
            dag_dirs=[self.sample_dag],
            target_dir=self.test_target,
            poll_interval=0.1,
        )
        watcher.initial_sync()

        # Check affected DAGs detection
        # If common_utils/date.py changes, partner_billing_etl should be affected
        changed_file = "dags/common_utils/date.py"
        affected = watcher.find_affected_dags([changed_file])
        self.assertIn(self.sample_dag, affected)

        # Non-dependency file should not affect the DAG
        unrelated_file = "docker/artifacts/requirements.txt"
        affected_unrelated = watcher.find_affected_dags([unrelated_file])
        self.assertEqual(len(affected_unrelated), 0)


if __name__ == "__main__":
    unittest.main()
