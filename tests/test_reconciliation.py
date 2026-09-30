import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from src.domain import (
    Actor,
    ConflictError,
    PermissionDenied,
    ReconciliationConflict,
    ValidationError,
)
from src.reconciliation import (
    EXC_CONFIRMED,
    EXC_RELEASED,
    EXC_RESOLVED,
    REC_MATCHED,
    REC_PENDING_REVIEW,
    REC_UNMATCHED,
)
from src.repository import SQLiteRepository
from src.rules import RuleEngine, reconciliation_deviation
from src.service import DomainService


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "recon.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.operator = Actor("qc-operator", "operator")
        self.analyst = Actor("qc-analyst", "analyst")
        self.reporter = Actor("lab-interface", "reporter")
        self._setup_fixtures()

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_fixtures(self, reconcile_abs_limit=None):
        assay_data = {
            "name": "Glucose",
            "unit": "mmol/L",
            "allowed_low": 3.9,
            "allowed_high": 6.1,
        }
        if reconcile_abs_limit is not None:
            assay_data["reconcile_abs_limit"] = reconcile_abs_limit
        self.assay = self.service.create(self.supervisor, "assay", assay_data)
        self.lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {
                "assay_id": self.assay["id"],
                "lot_no": "LOT-1",
                "target": 5.0,
                "sd": 0.1,
                "expires_at": "2099-01-01",
            },
        )
        self.lot = self.service.transition(
            self.supervisor, self.lot["id"], "activate", {"activated_by": "qc-1"}
        )
        self.instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )

    def _create_run_and_batch(self, qc_value=5.02, batch_no="B-2026-001", run_at="2026-09-27T08:05:00Z"):
        run = self.service.create(
            self.operator,
            "qc_run",
            {
                "assay_id": self.assay["id"],
                "qc_lot_id": self.lot["id"],
                "instrument_id": self.instrument["id"],
                "value": qc_value,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        run = self.service.transition(
            self.operator, run["id"], "evaluate", {"evaluated_by": "qc-1"}
        )
        self.assertEqual(run["status"], "accepted")
        batch = self.service.create(
            self.operator,
            "result_batch",
            {
                "assay_id": self.assay["id"],
                "instrument_id": self.instrument["id"],
                "qc_run_id": run["id"],
                "batch_no": batch_no,
                "run_at": run_at,
                "patient_count": 12,
            },
        )
        return run, batch

    def _report_payload(self, value, batch_no="B-2026-001", **extra):
        payload = {
            "assay_id": self.assay["id"],
            "instrument_id": self.instrument["id"],
            "batch_no": batch_no,
            "value": value,
            "reported_at": "2026-09-27T09:00:00Z",
            "source": "peer-lab",
        }
        payload.update(extra)
        return payload

    # ------------------------------------------------------------------ matched

    def test_report_within_limit_matches_and_keeps_batch_releaseable(self):
        run, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.05)
        )
        self.assertFalse(result["duplicate"])
        self.assertEqual(result["report"]["status"], "reconciled")
        self.assertEqual(result["reconciliation"]["status"], REC_MATCHED)
        self.assertIsNone(result["review_exception"])
        deviation = result["reconciliation"]["data"]["deviation"]
        self.assertTrue(deviation["within_limit"])
        self.assertAlmostEqual(deviation["limit"], 0.2)
        batch = self.service.get(batch["id"])
        self.assertEqual(batch["status"], "waiting")
        released = self.service.transition(
            self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"}
        )
        self.assertEqual(released["status"], "released")

    # ----------------------------------------------------- over-limit + two-person

    def test_deviation_over_limit_freezes_batch_and_requires_two_people(self):
        run, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.9)
        )
        reconciliation = result["reconciliation"]
        exception = result["review_exception"]
        self.assertEqual(reconciliation["status"], REC_PENDING_REVIEW)
        self.assertEqual(exception["status"], "open")
        self.assertEqual(exception["data"]["frozen_batch_ids"], [batch["id"]])
        batch = self.service.get(batch["id"])
        self.assertEqual(batch["status"], "frozen")

        # Frozen batches cannot slip through the normal release action.
        with self.assertRaises(Exception):
            self.service.transition(
                self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"}
            )

        # First person confirms.
        first = self.service.confirm_review_exception(
            self.operator, exception["id"], note="value re-measured, accept"
        )
        self.assertEqual(first["exception"]["status"], EXC_CONFIRMED)

        # Same person cannot count twice.
        with self.assertRaises(ReconciliationConflict):
            self.service.confirm_review_exception(
                self.operator, exception["id"], note="double sign"
            )

        # Second, different person confirms -> resolved.
        second = self.service.resolve_review_exception(
            self.analyst, exception["id"], note="second independent review"
        )
        self.assertEqual(second["exception"]["status"], EXC_RESOLVED)

        # Release before supervisor role is denied.
        with self.assertRaises(PermissionDenied):
            self.service.release_review_exception(
                self.operator, exception["id"], note="release"
            )

        # Release after two confirmations unfreezes the batch.
        released = self.service.release_review_exception(
            self.supervisor, exception["id"], note="supervisor release"
        )
        self.assertEqual(released["exception"]["status"], EXC_RELEASED)
        batch = self.service.get(batch["id"])
        self.assertEqual(batch["status"], "released")

    def test_release_blocked_until_both_confirmations(self):
        _, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.9)
        )
        exception_id = result["review_exception"]["id"]
        with self.assertRaises(ConflictError):
            self.service.release_review_exception(
                self.supervisor, exception_id, note="too early"
            )
        self.service.confirm_review_exception(
            self.operator, exception_id, note="one"
        )
        with self.assertRaises(ConflictError):
            self.service.release_review_exception(
                self.supervisor, exception_id, note="still too early"
            )
        self.assertEqual(self.service.get(batch["id"])["status"], "frozen")

    # --------------------------------------------------------------- idempotency

    def test_repeated_import_is_processed_once(self):
        self._create_run_and_batch(qc_value=5.02)
        payload = self._report_payload(5.9)
        first = self.service.import_external_report(self.reporter, payload)
        second = self.service.import_external_report(self.reporter, payload)
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["report"]["id"], second["report"]["id"])
        self.assertEqual(
            first["reconciliation"]["id"], second["reconciliation"]["id"]
        )
        self.assertEqual(first["review_exception"]["id"], second["review_exception"]["id"])

        reports = self.service.list("external_report")
        reconciliations = self.service.list("reconciliation")
        exceptions = self.service.list("review_exception")
        self.assertEqual(len(reports), 1)
        self.assertEqual(len(reconciliations), 1)
        self.assertEqual(len(exceptions), 1)

        actions = [
            row["action"]
            for row in self.service.audit_log(first["report"]["id"])
        ]
        self.assertEqual(actions, ["import", "reconcile"])
        deviation_rows = [
            row
            for row in self.service.audit_log()
            if row["entity_id"] == reconciliations[0]["id"]
        ]
        self.assertEqual(len(deviation_rows), 1)

    def test_idempotency_key_also_dedupes(self):
        self._create_run_and_batch()
        payload = self._report_payload(5.05)
        first = self.service.import_external_report(
            self.reporter, payload, idempotency_key="import-001"
        )
        second = self.service.import_external_report(
            self.reporter, payload, idempotency_key="import-001"
        )
        self.assertEqual(first["report"]["id"], second["report"]["id"])

    # ------------------------------------------------------------ retry safety

    def test_failed_import_is_fully_retryable(self):
        run, batch = self._create_run_and_batch(qc_value=5.02)
        payload = self._report_payload(5.9)
        audit_count_before = len(self.service.audit_log())

        # Force the processing step to fail after the natural key has been claimed
        # inside the transaction. Everything must roll back together.
        from src import reconciliation as recon_module

        with mock.patch(
            "src.reconciliation.reconciliation_deviation",
            side_effect=RuntimeError("simulated mid-processing failure"),
        ):
            with self.assertRaises(RuntimeError):
                self.service.import_external_report(self.reporter, payload)

        # No partial report, reconciliation, exception, frozen batch or audit row.
        self.assertEqual(self.service.list("external_report"), [])
        self.assertEqual(self.service.list("reconciliation"), [])
        self.assertEqual(self.service.list("review_exception"), [])
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")
        # The claimed natural key must also have rolled back (no new audit rows).
        self.assertEqual(len(self.service.audit_log()), audit_count_before)

        # Retry with the identical report succeeds and counts exactly once.
        result = self.service.import_external_report(self.reporter, payload)
        self.assertEqual(result["reconciliation"]["status"], REC_PENDING_REVIEW)
        self.assertEqual(len(self.service.list("external_report")), 1)
        self.assertEqual(len(self.service.list("reconciliation")), 1)
        self.assertEqual(self.service.get(batch["id"])["status"], "frozen")

    # ------------------------------------------------- concurrent confirmations

    def test_concurrent_confirmations_loser_gets_latest_and_conflicts(self):
        _, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.9)
        )
        exception_id = result["review_exception"]["id"]

        outcome = {}

        def confirm(actor):
            try:
                self.service.confirm_review_exception(
                    actor, exception_id, note="parallel confirm"
                )
                outcome[actor.user_id] = "ok"
            except ReconciliationConflict as exc:
                outcome[actor.user_id] = ("conflict", exc)
            except Exception as exc:  # pragma: no cover - diagnostic
                outcome[actor.user_id] = ("error", exc)

        t1 = threading.Thread(target=confirm, args=(self.operator,))
        t2 = threading.Thread(target=confirm, args=(self.analyst,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        results = list(outcome.values())
        oks = [item for item in results if item == "ok"]
        conflicts = [item for item in results if isinstance(item, tuple) and item[0] == "conflict"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(conflicts), 1)
        loser = conflicts[0][1]
        self.assertEqual(loser.latest["exception"]["status"], EXC_CONFIRMED)
        self.assertTrue(
            any(item["type"] == "already_confirmed" for item in loser.conflicts)
        )
        # Only one confirmation is stored, no double counting.
        exception = self.service.get(exception_id)
        self.assertEqual(len(exception["data"]["confirmations"]), 1)

    def test_stale_version_on_release_conflict(self):
        _, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.9)
        )
        exception = result["review_exception"]
        self.service.confirm_review_exception(
            self.operator, exception["id"], note="one"
        )
        self.service.resolve_review_exception(
            self.analyst, exception["id"], note="two"
        )
        # Client holds version 1 but the exception has moved on.
        with self.assertRaises(ReconciliationConflict) as caught:
            self.service.release_review_exception(
                self.supervisor, exception["id"], note="late", expected_version=1
            )
        self.assertEqual(
            caught.exception.latest["exception"]["status"], EXC_RESOLVED
        )

    def test_release_rolls_back_when_a_held_batch_changed(self):
        _, batch = self._create_run_and_batch(qc_value=5.02)
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.9)
        )
        exception_id = result["review_exception"]["id"]
        self.service.confirm_review_exception(
            self.operator, exception_id, note="one"
        )
        self.service.resolve_review_exception(
            self.analyst, exception_id, note="two"
        )
        # Simulate the held batch being moved out of frozen by another path.
        batch = self.service.get(batch["id"])
        self.service.repository.update_entity(batch["id"], batch["version"], "waiting", batch["data"])

        with self.assertRaises(ReconciliationConflict) as caught:
            self.service.release_review_exception(
                self.supervisor, exception_id, note="release"
            )
        self.assertTrue(
            any(item["type"] == "batch_state_changed" for item in caught.exception.conflicts)
        )
        # Nothing was partially released: exception and batch both stay put.
        self.assertEqual(self.service.get(exception_id)["status"], EXC_RESOLVED)
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")
        # Once the batch is frozen again, the release succeeds.
        self.service.repository.update_entity(batch["id"], batch["version"] + 1, "frozen", batch["data"])
        released = self.service.release_review_exception(
            self.supervisor, exception_id, note="release"
        )
        self.assertEqual(released["exception"]["status"], EXC_RELEASED)
        self.assertEqual(self.service.get(batch["id"])["status"], "released")

    # --------------------------------------------------------- unmatched / retry

    def test_unmatched_report_can_be_reconciled_later(self):
        # Report arrives before any local patient batch with that batch number.
        result = self.service.import_external_report(
            self.reporter, self._report_payload(5.05, batch_no="B-FUTURE")
        )
        self.assertEqual(result["report"]["status"], "received")
        self.assertEqual(result["reconciliation"]["status"], REC_UNMATCHED)
        self.assertIn("result_batch", result["reconciliation"]["data"]["missing"])
        self.assertIsNone(result["review_exception"])

        # Local batch appears, then the old report is reconciled explicitly.
        _, batch = self._create_run_and_batch(
            qc_value=5.02, batch_no="B-FUTURE"
        )
        again = self.service.reconcile_external_report(
            self.operator, result["report"]["id"]
        )
        self.assertEqual(again["report"]["status"], "reconciled")
        self.assertEqual(again["reconciliation"]["status"], REC_MATCHED)
        # The unmatched reconciliation is kept for history but marked superseded.
        all_recon = self.service.list("reconciliation")
        statuses = {item["status"] for item in all_recon}
        self.assertIn("superseded", statuses)
        self.assertEqual(self.service.get(batch["id"])["status"], "waiting")

    # -------------------------------------------------------- history queries

    def test_old_batches_stay_queryable_after_hold_and_release(self):
        run, batch = self._create_run_and_batch(qc_value=5.02, batch_no="B-OLD")
        self.service.import_external_report(
            self.reporter, self._report_payload(5.9, batch_no="B-OLD")
        )
        frozen = self.service.get(batch["id"])
        self.assertEqual(frozen["status"], "frozen")
        exception = self.service.list("review_exception")[0]
        self.service.confirm_review_exception(
            self.operator, exception["id"], note="one"
        )
        self.service.resolve_review_exception(
            self.analyst, exception["id"], note="two"
        )
        self.service.release_review_exception(
            self.supervisor, exception["id"], note="release"
        )
        # Old batch remains retrievable and the history is intact.
        released = self.service.get(batch["id"])
        self.assertEqual(released["status"], "released")
        self.assertEqual(
            released["data"]["released_exception_id"], exception["id"]
        )
        audit_actions = [row["action"] for row in self.service.audit_log(batch["id"])]
        self.assertIn("freeze", audit_actions)
        self.assertIn("release", audit_actions)

    # ------------------------------------------------------------- validation

    def test_import_requires_known_assay_instrument_and_numeric_value(self):
        base = self._report_payload(5.05)
        bad = dict(base)
        bad["assay_id"] = "missing"
        with self.assertRaises(ValidationError):
            self.service.import_external_report(self.reporter, bad)
        bad = dict(base)
        bad["value"] = "not-a-number"
        with self.assertRaises(ValidationError):
            self.service.import_external_report(self.reporter, bad)
        with self.assertRaises(PermissionDenied):
            self.service.import_external_report(Actor("viewer", "viewer"), base)

    def test_explicit_assay_limit_overrides_sd_default(self):
        # Recreate the fixtures with an explicit absolute limit of 0.5.
        self.tmp2 = tempfile.TemporaryDirectory()
        service = DomainService(
            SQLiteRepository(Path(self.tmp2.name) / "limit.db"), RuleEngine()
        )
        sup = Actor("sup", "supervisor")
        assay = service.create(
            sup,
            "assay",
            {
                "name": "A",
                "unit": "u",
                "allowed_low": 0,
                "allowed_high": 10,
                "reconcile_abs_limit": 0.5,
            },
        )
        lot = service.create(
            sup,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "L", "target": 5, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = service.transition(sup, lot["id"], "activate", {"activated_by": "a"})
        instrument = service.create(
            sup,
            "instrument",
            {"name": "I", "serial": "S", "calibration_due": "2099-01-01"},
        )
        run = service.create(
            sup,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": 5.0,
                "run_at": "2026-09-27T08:00:00Z",
            },
        )
        run = service.transition(sup, run["id"], "evaluate", {"evaluated_by": "a"})
        service.create(
            sup,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "batch_no": "B1",
                "run_at": "2026-09-27T08:05:00Z",
                "patient_count": 1,
            },
        )
        result = service.import_external_report(
            Actor("rep", "reporter"),
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "batch_no": "B1",
                "value": 5.4,
                "reported_at": "2026-09-27T09:00:00Z",
            },
        )
        deviation = result["reconciliation"]["data"]["deviation"]
        self.assertEqual(deviation["limit_source"], "assay_reconcile_abs_limit")
        self.assertAlmostEqual(deviation["limit"], 0.5)
        self.assertTrue(deviation["within_limit"])
        self.tmp2.cleanup()


class DeviationRuleTest(unittest.TestCase):
    def test_within_and_over_limit(self):
        ok = reconciliation_deviation(5.05, 5.02, limit=0.2)
        self.assertTrue(ok["within_limit"])
        self.assertAlmostEqual(ok["deviation"], 0.03)
        bad = reconciliation_deviation(5.9, 5.02, limit=0.2)
        self.assertFalse(bad["within_limit"])
        self.assertAlmostEqual(bad["abs_deviation"], 0.88)

    def test_unconfigured_limit_is_indeterminate(self):
        result = reconciliation_deviation(9.0, 5.0)
        self.assertIsNone(result["within_limit"])
        self.assertEqual(result["limit_source"], "unconfigured")

    def test_non_numeric_raises(self):
        with self.assertRaises(ValidationError):
            reconciliation_deviation("oops", 5.0, limit=0.2)


if __name__ == "__main__":
    unittest.main()
