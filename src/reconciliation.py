from uuid import uuid4

from .domain import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ReconciliationConflict,
    ValidationError,
)
from .repository import SQLiteRepository
from .rules import reconciliation_deviation

REPORT_KIND = "external_report"
RECONCILIATION_KIND = "reconciliation"
EXCEPTION_KIND = "review_exception"

REPORT_RECEIVED = "received"
REPORT_RECONCILED = "reconciled"

REC_MATCHED = "matched"
REC_PENDING_REVIEW = "pending_review"
REC_UNMATCHED = "unmatched"

EXC_OPEN = "open"
EXC_CONFIRMED = "confirmed"
EXC_RESOLVED = "resolved"
EXC_RELEASED = "released"

REPORT_IMPORTER_ROLES = ("reporter", "operator", "coordinator", "supervisor", "admin")
EXCEPTION_CONFIRM_ROLES = (
    "operator",
    "analyst",
    "coordinator",
    "supervisor",
    "admin",
)
EXCEPTION_RELEASE_ROLES = ("supervisor", "admin")

DEFAULT_SD_FACTOR = 2.0


class ReconciliationService:
    """Pairs external reports with local QC and patient result batches."""

    def __init__(self, repository, audit):
        self.repository = repository
        self.audit = audit

    # ------------------------------------------------------------------ helpers

    def _find(self, kind, field, value):
        return self.repository.find_entities(kind, field, value)

    @staticmethod
    def _find_conn(connection, kind, field, value):
        rows = SQLiteRepository.conn_list_entities(connection, kind=kind)
        return [
            entity
            for entity in rows
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    @staticmethod
    def conn_list(connection, kind):
        return SQLiteRepository.conn_list_entities(connection, kind=kind)

    @staticmethod
    def _report_key(assay_id, instrument_id, batch_no):
        return "%s|%s|%s" % (assay_id, instrument_id, batch_no)

    def _audit(self, connection, entity_id, actor, action, from_status, to_status, detail=None):
        self.repository.conn_append_audit(
            connection,
            entity_id,
            actor.user_id,
            actor.role,
            action,
            from_status,
            to_status,
            detail or {},
        )

    def _require_roles(self, actor, roles):
        if actor.role not in roles:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    def _get_report(self, report_id):
        report = self.repository.get_entity(report_id)
        if not report or report["kind"] != REPORT_KIND:
            raise NotFoundError("external report not found: " + str(report_id))
        return report

    # ------------------------------------------------------------- matching view

    def _snapshot(self, connection, report):
        """Resolve the local batches/QC runs a report reconciles against."""
        data = report["data"]
        assay_id = data["assay_id"]
        instrument_id = data["instrument_id"]
        batch_no = data["batch_no"]

        assay = self._find_conn(connection, "assay", "id", assay_id)
        assay = assay[0] if assay else None
        instrument = self._find_conn(connection, "instrument", "id", instrument_id)
        instrument = instrument[0] if instrument else None

        batches = [
            batch
            for batch in self.conn_list(connection, "result_batch")
            if batch["data"].get("assay_id") == assay_id
            and batch["data"].get("instrument_id") == instrument_id
            and str(batch["data"].get("batch_no") or "") == str(batch_no)
        ]
        batches.sort(key=lambda item: (str(item["data"].get("run_at") or ""), item["id"]))

        if not assay or not instrument or not batches:
            missing = []
            if not assay:
                missing.append("assay")
            if not instrument:
                missing.append("instrument")
            if not batches:
                missing.append("result_batch")
            return {"status": REC_UNMATCHED, "missing": missing, "batches": []}

        primary = None
        hint_run_id = data.get("qc_run_id")
        if hint_run_id:
            for batch in batches:
                if batch["data"].get("qc_run_id") == hint_run_id:
                    primary = batch
                    break
        if primary is None:
            primary = batches[-1]

        qc_run = self._find_conn(connection, "qc_run", "id", primary["data"].get("qc_run_id"))
        qc_run = qc_run[0] if qc_run else None
        qc_lot = (
            self._find_conn(connection, "qc_lot", "id", qc_run["data"].get("qc_lot_id"))
            if qc_run
            else []
        )
        qc_lot = qc_lot[0] if qc_lot else None

        local_value = data.get("local_value")
        if local_value is None and qc_run is not None:
            local_value = qc_run["data"].get("value")
        if local_value is None:
            return {
                "status": REC_UNMATCHED,
                "missing": ["local_reference_value"],
                "batches": batches,
                "primary_batch": primary,
            }

        limit = assay["data"].get("reconcile_abs_limit")
        limit_source = "assay_reconcile_abs_limit"
        if limit is None and qc_lot is not None:
            limit = DEFAULT_SD_FACTOR * float(qc_lot["data"]["sd"])
            limit_source = "qc_sd_x_%s" % DEFAULT_SD_FACTOR

        deviation = reconciliation_deviation(
            data.get("value"),
            local_value,
            limit=limit,
            limit_source=limit_source,
        )
        status = REC_MATCHED if deviation["within_limit"] is not False else REC_PENDING_REVIEW
        return {
            "status": status,
            "missing": [],
            "batches": batches,
            "primary_batch": primary,
            "qc_run_id": qc_run["id"] if qc_run else None,
            "qc_lot_id": qc_lot["id"] if qc_lot else None,
            "local_value": local_value,
            "deviation": deviation,
        }

    # ------------------------------------------------- reconciliation application

    def _freeze_for_exception(self, connection, batches, exception_id, actor):
        frozen_batch_ids = []
        unavailable_batches = []
        reason = "external report deviation pending review"
        for batch in batches:
            if batch["status"] == "waiting":
                patch = dict(batch["data"])
                patch["frozen_by_exception"] = exception_id
                self.repository.conn_update_entity(
                    connection,
                    batch["id"],
                    batch["version"],
                    "frozen",
                    patch,
                )
                frozen_batch_ids.append(batch["id"])
                self._audit(
                    connection,
                    batch["id"],
                    actor,
                    "freeze",
                    "waiting",
                    "frozen",
                    {"reason": reason, "exception_id": exception_id},
                )
            else:
                unavailable_batches.append({"id": batch["id"], "status": batch["status"]})
        return frozen_batch_ids, unavailable_batches

    def _process(self, connection, report, actor):
        """Run the deviation comparison and all side effects for one report."""
        snapshot = self._snapshot(connection, report)
        batches = snapshot.pop("batches")
        primary = snapshot.pop("primary_batch", None)
        status = snapshot.pop("status")
        missing = snapshot.pop("missing")

        reconciliation_id = str(uuid4())
        reconciliation_data = {
            "report_id": report["id"],
            "assay_id": report["data"]["assay_id"],
            "instrument_id": report["data"]["instrument_id"],
            "batch_no": report["data"]["batch_no"],
            "missing": missing,
            "result_batch_ids": [batch["id"] for batch in batches],
            "primary_batch_id": primary["id"] if primary else None,
        }
        reconciliation_data.update(snapshot)
        reconciliation = self.repository.conn_insert_entity(
            connection,
            reconciliation_id,
            RECONCILIATION_KIND,
            status,
            reconciliation_data,
            actor.user_id,
        )
        self._audit(
            connection,
            reconciliation_id,
            actor,
            "reconcile",
            None,
            status,
            {"report_id": report["id"], "deviation": snapshot.get("deviation")},
        )

        exception = None
        if status == REC_PENDING_REVIEW:
            exception_id = str(uuid4())
            exception_data = {
                "report_id": report["id"],
                "reconciliation_id": reconciliation_id,
                "assay_id": report["data"]["assay_id"],
                "instrument_id": report["data"]["instrument_id"],
                "batch_no": report["data"]["batch_no"],
                "deviation": snapshot.get("deviation"),
                "confirmations": [],
                "frozen_batch_ids": [],
                "unavailable_batches": [],
            }
            exception = self.repository.conn_insert_entity(
                connection,
                exception_id,
                EXCEPTION_KIND,
                EXC_OPEN,
                exception_data,
                actor.user_id,
            )
            self._audit(
                connection,
                exception_id,
                actor,
                "open",
                None,
                EXC_OPEN,
                {"report_id": report["id"], "reconciliation_id": reconciliation_id},
            )
            frozen, unavailable = self._freeze_for_exception(
                connection, batches, exception_id, actor
            )
            exception_data["frozen_batch_ids"] = frozen
            exception_data["unavailable_batches"] = unavailable
            exception = self.repository.conn_update_entity(
                connection, exception_id, exception["version"], EXC_OPEN, exception_data
            )

        if status in (REC_MATCHED, REC_PENDING_REVIEW):
            report_patch = dict(report["data"])
            report_patch["reconciliation_id"] = reconciliation_id
            if exception:
                report_patch["review_exception_id"] = exception["id"]
            report = self.repository.conn_update_entity(
                connection, report["id"], report["version"], REPORT_RECONCILED, report_patch
            )
            self._audit(
                connection,
                report["id"],
                actor,
                "reconcile",
                REPORT_RECEIVED,
                REPORT_RECONCILED,
                {"reconciliation_id": reconciliation_id, "outcome": status},
            )

        return report, reconciliation, exception

    # ------------------------------------------------------------------- import

    def import_report(self, actor, payload, idempotency_key=None):
        """Ingest one external report keyed by assay, instrument and batch number.

        Re-importing the same report returns the stored conclusion untouched: the
        natural key is claimed inside the processing transaction, so a crash before
        commit leaves no partial deviation/audit rows and the import can be retried.
        """
        self._require_roles(actor, REPORT_IMPORTER_ROLES)
        for field in ("assay_id", "instrument_id", "batch_no", "value", "reported_at"):
            if payload.get(field) in (None, ""):
                raise ValidationError("missing required field: " + field)
        try:
            float(payload["value"])
        except (TypeError, ValueError):
            raise ValidationError("external report value must be numeric")

        assay = self._find("assay", "id", payload["assay_id"])
        if not assay:
            raise ValidationError("assay does not exist")
        instrument = self._find("instrument", "id", payload["instrument_id"])
        if not instrument:
            raise ValidationError("instrument does not exist")

        report_key = self._report_key(
            payload["assay_id"], payload["instrument_id"], str(payload["batch_no"])
        )
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                report = self.repository.get_entity(existing)
                if report:
                    return self._bundle(report, duplicate=True)

        report_id = str(payload.get("id") or uuid4())
        if self.repository.get_entity(report_id):
            raise ConflictError("entity already exists: " + report_id)

        report_data = {
            field: payload[field]
            for field in ("assay_id", "instrument_id", "batch_no", "value", "reported_at")
        }
        for optional in ("source", "unit", "local_value", "qc_run_id"):
            if payload.get(optional) is not None:
                report_data[optional] = payload[optional]
        report_data["report_key"] = report_key
        report_data["value"] = float(report_data["value"])

        with self.repository.transaction() as connection:
            claimed_by = self.repository.conn_reserve_report_import(
                connection, report_key, report_id
            )
            if claimed_by:
                existing_report = self.repository.conn_get_entity(connection, claimed_by)
                return self._bundle(existing_report, duplicate=True)

            report = self.repository.conn_insert_entity(
                connection,
                report_id,
                REPORT_KIND,
                REPORT_RECEIVED,
                report_data,
                actor.user_id,
            )
            self._audit(
                connection,
                report_id,
                actor,
                "import",
                None,
                REPORT_RECEIVED,
                {"report_key": report_key, "source": report_data.get("source")},
            )
            report, reconciliation, exception = self._process(connection, report, actor)

        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, report["id"])
        return self._bundle(report, reconciliation=reconciliation, exception=exception)

    # ------------------------------------------------------------ re-reconcile

    def reconcile_report(self, actor, report_id, expected_version=None):
        """Explicitly reconcile a report that could not be paired when first imported."""
        self._require_roles(actor, REPORT_IMPORTER_ROLES)
        report = self._get_report(report_id)
        if expected_version is not None and report["version"] != int(expected_version):
            raise self._conflict(report, "report version changed before reconcile")

        existing = self._find(RECONCILIATION_KIND, "report_id", report_id)
        existing = [item for item in existing if item["status"] != REC_UNMATCHED]
        if existing:
            # Deviation was already counted once; the conclusion never changes here.
            return self._bundle(report, duplicate=True)

        with self.repository.transaction() as connection:
            report = self.repository.conn_get_entity(connection, report_id)
            unmatched = self.repository.conn_find_entities(
                connection, RECONCILIATION_KIND, "report_id", report_id
            )
            if [item for item in unmatched if item["status"] != REC_UNMATCHED]:
                return self._bundle(report, duplicate=True)
            for item in unmatched:
                self.repository.conn_update_entity(
                    connection,
                    item["id"],
                    item["version"],
                    "superseded",
                    dict(item["data"], superseded_by_reason="matched on retry"),
                )
                self._audit(
                    connection, item["id"], actor, "supersede", item["status"], "superseded", {}
                )
            report, reconciliation, exception = self._process(connection, report, actor)

        return self._bundle(report, reconciliation=reconciliation, exception=exception)

    # ----------------------------------------------------- two-person review flow

    def _get_exception(self, exception_id):
        exception = self.repository.get_entity(exception_id)
        if not exception or exception["kind"] != EXCEPTION_KIND:
            raise NotFoundError("review exception not found: " + str(exception_id))
        return exception

    def _bundle_exception(self, exception):
        report = self.repository.get_entity(exception["data"].get("report_id"))
        reconciliation = self.repository.get_entity(
            exception["data"].get("reconciliation_id")
        )
        return {
            "exception": exception,
            "report": report,
            "reconciliation": reconciliation,
        }

    def _conflict(self, exception_or_report, message):
        if exception_or_report.get("kind") == EXCEPTION_KIND:
            bundle = self._bundle_exception(exception_or_report)
            conflicts = []
            for confirmation in exception_or_report["data"].get("confirmations", []):
                conflicts.append(
                    {
                        "type": "already_confirmed",
                        "user_id": confirmation["user_id"],
                        "note": confirmation.get("note"),
                    }
                )
            for batch_id in exception_or_report["data"].get("frozen_batch_ids", []):
                batch = self.repository.get_entity(batch_id)
                if batch and batch["status"] != "frozen":
                    conflicts.append(
                        {
                            "type": "batch_state_changed",
                            "batch_id": batch_id,
                            "status": batch["status"],
                        }
                    )
            return ReconciliationConflict(
                message, latest={"exception": bundle["exception"]}, conflicts=conflicts
            )
        return ReconciliationConflict(message, latest={"report": exception_or_report}, conflicts=[])

    def confirm_exception(self, actor, exception_id, note="", expected_version=None):
        """Record one of the two independent confirmations required before release."""
        self._require_roles(actor, EXCEPTION_CONFIRM_ROLES)
        exception = self._get_exception(exception_id)
        if not str(note or "").strip():
            raise ValidationError("confirmation note is required")

        with self.repository.transaction() as connection:
            exception = self.repository.conn_get_entity(connection, exception_id)
            current_version = exception["version"]
            if expected_version is not None and current_version != int(expected_version):
                raise self._conflict(exception, "a concurrent confirmation arrived first")
            if exception["status"] in (EXC_RESOLVED, EXC_RELEASED):
                raise self._conflict(
                    exception, "review already reached conclusion: " + exception["status"]
                )

            confirmations = list(exception["data"].get("confirmations") or [])
            if any(item["user_id"] == actor.user_id for item in confirmations):
                raise self._conflict(
                    exception, "user already confirmed this exception"
                )
            if exception["status"] == EXC_CONFIRMED:
                raise self._conflict(
                    exception, "a concurrent confirmation arrived first"
                )

            confirmations.append(
                {"user_id": actor.user_id, "role": actor.role, "note": note}
            )
            data = dict(exception["data"])
            data["confirmations"] = confirmations
            next_status = EXC_CONFIRMED
            exception = self.repository.conn_update_entity(
                connection, exception_id, current_version, next_status, data
            )
            self._audit(
                connection,
                exception_id,
                actor,
                "confirm",
                "open" if len(confirmations) == 1 else EXC_CONFIRMED,
                next_status,
                {"note": note, "confirmation_count": len(confirmations)},
            )
        return self._bundle_exception(exception)

    def resolve_exception(self, actor, exception_id, note="", expected_version=None):
        """Second independent confirmation; the exception becomes releasable."""
        self._require_roles(actor, EXCEPTION_CONFIRM_ROLES)
        exception = self._get_exception(exception_id)
        if not str(note or "").strip():
            raise ValidationError("confirmation note is required")

        with self.repository.transaction() as connection:
            exception = self.repository.conn_get_entity(connection, exception_id)
            current_version = exception["version"]
            if expected_version is not None and current_version != int(expected_version):
                raise self._conflict(exception, "a concurrent confirmation arrived first")
            if exception["status"] in (EXC_RESOLVED, EXC_RELEASED):
                raise self._conflict(
                    exception, "review already reached conclusion: " + exception["status"]
                )
            if exception["status"] != EXC_CONFIRMED:
                raise ConflictError("first confirmation must be recorded before resolving")

            confirmations = list(exception["data"].get("confirmations") or [])
            if any(item["user_id"] == actor.user_id for item in confirmations):
                raise self._conflict(exception, "user already confirmed this exception")

            confirmations.append(
                {"user_id": actor.user_id, "role": actor.role, "note": note}
            )
            data = dict(exception["data"])
            data["confirmations"] = confirmations
            exception = self.repository.conn_update_entity(
                connection, exception_id, current_version, EXC_RESOLVED, data
            )
            self._audit(
                connection,
                exception_id,
                actor,
                "resolve",
                EXC_CONFIRMED,
                EXC_RESOLVED,
                {"note": note, "confirmation_count": len(confirmations)},
            )
        return self._bundle_exception(exception)

    def release_exception(self, actor, exception_id, note="", expected_version=None):
        """Release the held batches after two people have confirmed the exception."""
        self._require_roles(actor, EXCEPTION_RELEASE_ROLES)
        exception = self._get_exception(exception_id)
        if not str(note or "").strip():
            raise ValidationError("release note is required")

        with self.repository.transaction() as connection:
            exception = self.repository.conn_get_entity(connection, exception_id)
            current_version = exception["version"]
            if expected_version is not None and current_version != int(expected_version):
                raise self._conflict(exception, "a concurrent release request arrived first")
            if exception["status"] == EXC_RELEASED:
                return self._bundle_exception(exception)
            if exception["status"] != EXC_RESOLVED:
                raise ConflictError(
                    "two confirmations are required before release (status=%s)"
                    % exception["status"]
                )

            data = dict(exception["data"])
            held_batches = []
            conflicts = []
            for batch_id in data.get("frozen_batch_ids", []):
                batch = self.repository.conn_get_entity(connection, batch_id)
                if not batch:
                    conflicts.append({"type": "batch_missing", "batch_id": batch_id})
                elif batch["status"] != "frozen":
                    conflicts.append(
                        {"type": "batch_state_changed", "batch_id": batch_id, "status": batch["status"]}
                    )
                else:
                    held_batches.append(batch)

            # Every held batch must still be frozen; otherwise the release is
            # rejected wholesale so the caller re-reads the latest conclusion.
            if conflicts:
                raise ReconciliationConflict(
                    "some held batches are no longer releasable",
                    latest={"exception": exception},
                    conflicts=conflicts,
                )

            released_batch_ids = []
            for batch in held_batches:
                patch = dict(batch["data"])
                patch["released_by"] = actor.user_id
                patch["released_exception_id"] = exception_id
                self.repository.conn_update_entity(
                    connection, batch["id"], batch["version"], "released", patch
                )
                released_batch_ids.append(batch["id"])
                self._audit(
                    connection,
                    batch["id"],
                    actor,
                    "release",
                    "frozen",
                    "released",
                    {"reason": note, "exception_id": exception_id},
                )

            data["released_batch_ids"] = released_batch_ids
            data["release_note"] = note
            exception = self.repository.conn_update_entity(
                connection, exception_id, current_version, EXC_RELEASED, data
            )
            self._audit(
                connection,
                exception_id,
                actor,
                "release",
                EXC_RESOLVED,
                EXC_RELEASED,
                {"note": note, "released_batch_ids": released_batch_ids},
            )

        return self._bundle_exception(exception)

    # ------------------------------------------------------------------ reads

    def _bundle(self, report, duplicate=False, reconciliation=None, exception=None):
        if reconciliation is None:
            reconciliation = self.repository.get_entity(
                report["data"].get("reconciliation_id")
            )
        if exception is None:
            exception = self.repository.get_entity(
                report["data"].get("review_exception_id")
            )
        return {
            "duplicate": duplicate,
            "report": report,
            "reconciliation": reconciliation,
            "review_exception": exception,
        }
