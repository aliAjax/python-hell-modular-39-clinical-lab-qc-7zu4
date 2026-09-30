import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConcurrentModification, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "reconciliation.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.analyst = Actor("qc-analyst", "analyst")
        self.viewer = Actor("qc-viewer", "viewer")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup(self, local_value=5.0, deviation_limit=0.5, batch_no="B-001", with_run=True):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {
                "name": "Glucose",
                "unit": "mmol/L",
                "allowed_low": 3.9,
                "allowed_high": 6.1,
                "rule_config": {"deviation_limit": deviation_limit},
            },
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "qc-1"})
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        run = None
        batch = None
        if with_run:
            run = self.service.create(
                self.supervisor,
                "qc_run",
                {
                    "assay_id": assay["id"],
                    "qc_lot_id": lot["id"],
                    "instrument_id": instrument["id"],
                    "value": local_value,
                    "run_at": "2026-09-27T08:00:00Z",
                },
            )
            run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})
            batch = self.service.create(
                self.supervisor,
                "result_batch",
                {
                    "assay_id": assay["id"],
                    "instrument_id": instrument["id"],
                    "qc_run_id": run["id"],
                    "run_at": "2026-09-27T08:05:00Z",
                    "patient_count": 12,
                    "batch_no": batch_no,
                },
            )
        return assay, lot, instrument, run, batch

    def _import_report(self, assay, instrument, batch_no, reported_value):
        return self.service.create(
            self.supervisor,
            "external_report",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "batch_no": batch_no,
                "reported_value": reported_value,
                "reported_at": "2026-09-27T08:10:00Z",
            },
        )

    # ------------------------------------------------------------------
    # Import idempotency
    # ------------------------------------------------------------------
    def test_report_import_is_idempotent_by_natural_key(self):
        assay, _, instrument, _, _ = self._setup()
        first = self._import_report(assay, instrument, "B-001", 5.01)
        second = self._import_report(assay, instrument, "B-001", 5.01)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.service.list("external_report")), 1)

    def test_report_import_idempotency_key_header(self):
        assay, _, instrument, _, _ = self._setup()
        payload = {
            "assay_id": assay["id"],
            "instrument_id": instrument["id"],
            "batch_no": "B-009",
            "reported_value": 5.01,
            "reported_at": "2026-09-27T08:10:00Z",
        }
        first = self.service.create(self.supervisor, "external_report", payload, "fixed-key")
        second = self.service.create(self.supervisor, "external_report", payload, "fixed-key")
        self.assertEqual(first["id"], second["id"])

    def test_internal_kinds_cannot_be_created_directly(self):
        with self.assertRaises(ValidationError):
            self.service.create(self.supervisor, "reconciliation", {})
        with self.assertRaises(ValidationError):
            self.service.create(self.supervisor, "deviation_exception", {})

    # ------------------------------------------------------------------
    # Matched reconciliation
    # ------------------------------------------------------------------
    def test_reconcile_matched_keeps_batches_waiting(self):
        assay, _, instrument, _, batch = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.01)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        self.assertEqual(reconciliation["status"], "matched")
        self.assertEqual(reconciliation["data"]["conclusion"], "matched")
        self.assertIsNone(reconciliation["data"]["exception_id"])
        self.assertEqual(self.service.get(report["id"])["status"], "reconciled")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")
        self.assertEqual(self.service.list("deviation_exception"), [])

    # ------------------------------------------------------------------
    # Deviation, freeze and two-person release
    # ------------------------------------------------------------------
    def test_reconcile_deviation_freezes_batches(self):
        assay, _, instrument, _, batch = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        self.assertEqual(reconciliation["status"], "deviation")
        self.assertGreater(reconciliation["data"]["deviation"], reconciliation["data"]["limit"])
        self.assertEqual(self.service.get(report["id"])["status"], "deviation")
        self.assertEqual(self.service.get(batch["id"])["status"], "frozen")
        exceptions = self.service.list("deviation_exception")
        self.assertEqual(len(exceptions), 1)
        self.assertEqual(exceptions[0]["status"], "pending")
        self.assertEqual(exceptions[0]["data"]["result_batch_ids"], [batch["id"]])

    def test_two_person_confirmation_required_before_release(self):
        assay, _, instrument, _, batch = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        exception = self.service.get(reconciliation["data"]["exception_id"])

        # First confirmation keeps the exception pending.
        exception = self.service.transition(self.analyst, exception["id"], "confirm", {})
        self.assertEqual(exception["status"], "pending")
        self.assertEqual(len(exception["data"]["confirmations"]), 1)

        # One confirmation is not enough to release.
        with self.assertRaises(ConcurrentModification) as context:
            self.service.transition(self.supervisor, exception["id"], "release", {})
        self.assertIn("two confirmations required", " ".join(context.exception.conflicts))
        self.assertEqual(self.service.get(batch["id"])["status"], "frozen")

        # The same person cannot confirm twice.
        with self.assertRaises(ConcurrentModification) as context:
            self.service.transition(self.analyst, exception["id"], "confirm", {})
        self.assertIn("already confirmed by", " ".join(context.exception.conflicts))

        # A second, distinct person confirms -> confirmed.
        exception = self.service.transition(self.supervisor, exception["id"], "confirm", {})
        self.assertEqual(exception["status"], "confirmed")
        self.assertEqual(len(exception["data"]["confirmations"]), 2)

        # Release unfreezes the batch.
        exception = self.service.transition(self.supervisor, exception["id"], "release", {})
        self.assertEqual(exception["status"], "released")
        self.assertEqual(self.service.get(batch["id"])["status"], "released")

    def test_viewer_cannot_confirm_or_release(self):
        assay, _, instrument, _, _ = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        exception = self.service.get(reconciliation["data"]["exception_id"])
        with self.assertRaises(PermissionDenied):
            self.service.transition(self.viewer, exception["id"], "confirm", {})
        with self.assertRaises(PermissionDenied):
            self.service.transition(self.viewer, exception["id"], "release", {})

    # ------------------------------------------------------------------
    # Concurrent requests: later caller gets latest state + conflicts
    # ------------------------------------------------------------------
    def test_concurrent_confirm_returns_latest_state_and_conflicts(self):
        assay, _, instrument, _, _ = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        exception = self.service.get(reconciliation["data"]["exception_id"])
        stale_version = exception["version"]

        # Analyst wins the race.
        self.service.transition(self.analyst, exception["id"], "confirm", {})

        # Supervisor submits against the stale version -> conflict with latest.
        with self.assertRaises(ConcurrentModification) as context:
            self.service.transition(
                self.supervisor, exception["id"], "confirm", {}, expected_version=stale_version
            )
        error = context.exception
        self.assertIsNotNone(error.latest)
        self.assertEqual(error.latest["version"], stale_version + 1)
        self.assertEqual(len(error.latest["data"]["confirmations"]), 1)
        self.assertTrue(any("version conflict" in item for item in error.conflicts))

    def test_concurrent_release_returns_latest_state_and_conflicts(self):
        assay, _, instrument, _, _ = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        exception = self.service.get(reconciliation["data"]["exception_id"])
        self.service.transition(self.analyst, exception["id"], "confirm", {})
        exception = self.service.transition(self.supervisor, exception["id"], "confirm", {})
        stale_version = exception["version"]

        # First release wins.
        self.service.transition(self.supervisor, exception["id"], "release", {})

        # Second release against stale version -> latest state + conflicts.
        with self.assertRaises(ConcurrentModification) as context:
            self.service.transition(
                self.supervisor, exception["id"], "release", {}, expected_version=stale_version
            )
        error = context.exception
        self.assertEqual(error.latest["status"], "released")
        self.assertTrue(any("version conflict" in item for item in error.conflicts))

    # ------------------------------------------------------------------
    # Retry after failure
    # ------------------------------------------------------------------
    def test_failed_reconciliation_can_be_retried(self):
        assay, lot, instrument, _, _ = self._setup(with_run=False)
        report = self._import_report(assay, instrument, "B-001", 5.01)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        self.assertEqual(reconciliation["status"], "failed")
        self.assertEqual(self.service.get(report["id"])["status"], "failed")

        # Retry before any QC run exists stays failed.
        reconciliation = self.service.transition(self.supervisor, reconciliation["id"], "retry", {})
        self.assertEqual(reconciliation["status"], "failed")
        self.assertEqual(reconciliation["data"]["attempt"], 2)

        # A local QC run and patient batch arrive; retry now matches.
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": 5.0,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        run = self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 12,
                "batch_no": "B-001",
            },
        )
        reconciliation = self.service.transition(self.supervisor, reconciliation["id"], "retry", {})
        self.assertEqual(reconciliation["status"], "matched")
        self.assertEqual(reconciliation["data"]["attempt"], 3)
        self.assertEqual(self.service.get(report["id"])["status"], "reconciled")
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")

    def test_retry_matched_reconciliation_conflicts(self):
        assay, _, instrument, _, _ = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.01)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        self.assertEqual(reconciliation["status"], "matched")
        with self.assertRaises(ConcurrentModification):
            self.service.transition(self.supervisor, reconciliation["id"], "retry", {})

    # ------------------------------------------------------------------
    # Idempotent deviation / audit and historical queries
    # ------------------------------------------------------------------
    def test_reconcile_does_not_duplicate_deviation_or_audit(self):
        assay, _, instrument, _, batch = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        first = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        second = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.service.list("reconciliation")), 1)
        self.assertEqual(len(self.service.list("deviation_exception")), 1)
        self.assertEqual(self.service.get(batch["id"])["status"], "frozen")
        freeze_audits = [
            entry for entry in self.service.audit_log(batch["id"]) if entry["action"] == "freeze"
        ]
        self.assertEqual(len(freeze_audits), 1)

    def test_old_batches_remain_queryable_after_release(self):
        assay, _, instrument, _, batch = self._setup(local_value=5.0)
        report = self._import_report(assay, instrument, "B-001", 5.9)
        reconciliation = self.service.transition(self.supervisor, report["id"], "reconcile", {})
        exception = self.service.get(reconciliation["data"]["exception_id"])
        self.service.transition(self.analyst, exception["id"], "confirm", {})
        self.service.transition(self.supervisor, exception["id"], "confirm", {})
        self.service.transition(self.supervisor, exception["id"], "release", {})

        # The released batch is still individually readable and listable.
        fetched = self.service.get(batch["id"])
        self.assertEqual(fetched["status"], "released")
        listed = self.service.list("result_batch")
        self.assertIn(batch["id"], [item["id"] for item in listed])
        # The full audit trail for the report is intact.
        report_audit = self.service.audit_log(report["id"])
        self.assertTrue(any(entry["action"] == "create" for entry in report_audit))
        self.assertTrue(any(entry["action"] == "reconcile" for entry in report_audit))


if __name__ == "__main__":
    unittest.main()
