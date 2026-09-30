from uuid import uuid4

from .audit import AuditTrail
from .domain import ConcurrentModification, ConflictError, NotFoundError
from .repository import utcnow
from .rules import RuleEngine


class DomainService:
    # Absolute deviation limit used when an assay does not configure one.
    DEFAULT_DEVIATION_LIMIT = 0.5

    # (kind, action) -> handler; these workflows need orchestration beyond a status change.
    SPECIAL_ACTIONS = {
        ("external_report", "reconcile"): "_action_reconcile",
        ("reconciliation", "retry"): "_action_retry",
        ("deviation_exception", "confirm"): "_action_confirm",
        ("deviation_exception", "release"): "_action_release",
    }

    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------
    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        # External reports are warehoused by assay/instrument/batch_no; a duplicate
        # import is processed exactly once and returns the original report.
        if kind == "external_report":
            duplicate = self.repository.find_one(
                "external_report",
                assay_id=payload.get("assay_id"),
                instrument_id=payload.get("instrument_id"),
                batch_no=payload.get("batch_no"),
            )
            if duplicate:
                return duplicate
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    # ------------------------------------------------------------------
    # Transitions
    # ------------------------------------------------------------------
    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])
        handler = self.SPECIAL_ACTIONS.get((kind, action))
        if handler:
            return getattr(self, handler)(actor, entity, data or {}, expected_version)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    # ------------------------------------------------------------------
    # Reconciliation workflow
    # ------------------------------------------------------------------
    def _action_reconcile(self, actor, report, data, expected_version):
        self.rules.check_role(actor, "external_report", "reconcile")
        existing = self.repository.find_one("reconciliation", report_id=report["id"])
        if existing and existing["status"] != "failed":
            # Already reconciled (matched or deviation): idempotent no-op.
            return existing
        return self._run_reconciliation(actor, report, existing)

    def _action_retry(self, actor, reconciliation, data, expected_version):
        self.rules.check_role(actor, "reconciliation", "retry")
        if reconciliation["status"] != "failed":
            raise ConcurrentModification(
                "only a failed reconciliation can be retried",
                latest=reconciliation,
                conflicts=["reconciliation status is %s, not failed" % reconciliation["status"]],
            )
        report = self.repository.get_entity(reconciliation["data"].get("report_id"))
        if not report:
            raise NotFoundError("report not found for reconciliation: " + reconciliation["id"])
        return self._run_reconciliation(actor, report, reconciliation)

    def _run_reconciliation(self, actor, report, existing):
        assay = self.repository.get_entity(report["data"].get("assay_id"))

        def work(connection):
            data = report["data"]
            # Re-check under the write lock so concurrent reconcile/retry calls
            # cannot create a second reconciliation for the same report.
            prior = self.repository._find_one(connection, "reconciliation", report_id=report["id"])
            if prior and prior["status"] != "failed":
                return prior

            runs = self.repository._find_all(connection, "qc_run", assay_id=data["assay_id"])
            runs = [
                run for run in runs
                if run["data"].get("instrument_id") == data["instrument_id"]
                and run["status"] == "accepted"
            ]
            runs.sort(key=lambda run: str(run["data"].get("run_at", "")), reverse=True)
            qc_run = runs[0] if runs else None

            batches = self.repository._find_all(connection, "result_batch", assay_id=data["assay_id"])
            batches = [
                batch for batch in batches
                if batch["data"].get("instrument_id") == data["instrument_id"]
                and batch["data"].get("batch_no") == data["batch_no"]
                and batch["status"] == "waiting"
            ]

            if not qc_run:
                conclusion = "failed"
                deviation = None
                limit = None
                local_value = None
            else:
                reported = float(data["reported_value"])
                local_value = float(qc_run["data"]["value"])
                deviation = round(abs(reported - local_value), 4)
                configured = (assay or {}).get("data", {}).get("rule_config", {}) or {}
                limit_value = configured.get("deviation_limit", self.DEFAULT_DEVIATION_LIMIT)
                limit = float(limit_value) if limit_value is not None else self.DEFAULT_DEVIATION_LIMIT
                conclusion = "deviation" if deviation > limit else "matched"

            attempt = (int(prior["data"].get("attempt", 0)) + 1) if prior else 1
            recon_data = {
                "report_id": report["id"],
                "assay_id": data["assay_id"],
                "instrument_id": data["instrument_id"],
                "batch_no": data["batch_no"],
                "reported_value": data["reported_value"],
                "qc_run_id": qc_run["id"] if qc_run else None,
                "local_value": local_value,
                "deviation": deviation,
                "limit": limit,
                "result_batch_ids": [batch["id"] for batch in batches],
                "conclusion": conclusion,
                "attempt": attempt,
                "exception_id": None,
            }
            if prior:
                recon = self.repository._update_entity(connection, prior["id"], None, conclusion, recon_data)
            else:
                recon = self.repository._create_entity(
                    connection, str(uuid4()), "reconciliation", conclusion, recon_data, actor.user_id
                )

            report_status = {
                "matched": "reconciled",
                "deviation": "deviation",
                "failed": "failed",
            }[conclusion]
            report_data = dict(report["data"])
            report_data["reconciliation_id"] = recon["id"]
            report_data["last_conclusion"] = conclusion
            updated_report = self.repository._update_entity(
                connection, report["id"], None, report_status, report_data
            )

            self.repository._append_audit(
                connection,
                recon["id"],
                actor.user_id,
                actor.role,
                "retry" if prior else "reconcile",
                prior["status"] if prior else None,
                conclusion,
                {"attempt": attempt, "deviation": deviation, "limit": limit},
            )
            self.repository._append_audit(
                connection,
                report["id"],
                actor.user_id,
                actor.role,
                "reconcile",
                report["status"],
                report_status,
                {"conclusion": conclusion},
            )

            if conclusion == "deviation":
                exception = self.repository._find_one(
                    connection, "deviation_exception", report_id=report["id"]
                )
                if not exception:
                    exc_data = {
                        "report_id": report["id"],
                        "reconciliation_id": recon["id"],
                        "assay_id": data["assay_id"],
                        "instrument_id": data["instrument_id"],
                        "batch_no": data["batch_no"],
                        "deviation": deviation,
                        "limit": limit,
                        "reported_value": data["reported_value"],
                        "local_value": local_value,
                        "qc_run_id": qc_run["id"],
                        "result_batch_ids": [batch["id"] for batch in batches],
                        "confirmations": [],
                    }
                    exception = self.repository._create_entity(
                        connection, str(uuid4()), "deviation_exception", "pending", exc_data, actor.user_id
                    )
                    self.repository._append_audit(
                        connection,
                        exception["id"],
                        actor.user_id,
                        actor.role,
                        "create",
                        None,
                        "pending",
                        {"deviation": deviation, "limit": limit},
                    )
                # Freeze the linked patient batches until two-person release.
                for batch in batches:
                    batch_data = dict(batch["data"])
                    batch_data["frozen_by_report"] = report["id"]
                    batch_data["frozen_at"] = utcnow()
                    self.repository._update_entity(
                        connection, batch["id"], batch["version"], "frozen", batch_data
                    )
                    self.repository._append_audit(
                        connection,
                        batch["id"],
                        actor.user_id,
                        actor.role,
                        "freeze",
                        "waiting",
                        "frozen",
                        {"report_id": report["id"], "exception_id": exception["id"]},
                    )
                recon_data["exception_id"] = exception["id"]
                recon = self.repository._update_entity(
                    connection, recon["id"], None, conclusion, recon_data
                )
            return recon

        return self.repository.transact(work)

    # ------------------------------------------------------------------
    # Two-person confirmation and release
    # ------------------------------------------------------------------
    def _action_confirm(self, actor, exception, data, expected_version):
        self.rules.check_role(actor, "deviation_exception", "confirm")

        def work(connection):
            current = self.repository._get_entity(connection, exception["id"])
            confirmations = list(current["data"].get("confirmations") or [])
            business_conflicts = []
            if current["status"] not in ("pending", "confirmed"):
                business_conflicts.append("exception already %s" % current["status"])
            if any(item.get("user_id") == actor.user_id for item in confirmations):
                business_conflicts.append("already confirmed by %s" % actor.user_id)
            version_conflict = (
                expected_version is not None and int(expected_version) != current["version"]
            )
            if version_conflict:
                conflicts = [
                    "version conflict: expected %s, found %s"
                    % (expected_version, current["version"])
                ] + business_conflicts
                raise ConcurrentModification(
                    "confirmation conflict", latest=current, conflicts=conflicts
                )
            if business_conflicts:
                raise ConcurrentModification(
                    "confirmation conflict", latest=current, conflicts=business_conflicts
                )

            new_confirmations = confirmations + [
                {"user_id": actor.user_id, "role": actor.role, "confirmed_at": utcnow()}
            ]
            new_status = "confirmed" if len(new_confirmations) >= 2 else "pending"
            new_data = dict(current["data"])
            new_data["confirmations"] = new_confirmations
            updated = self.repository._update_entity(
                connection, exception["id"], current["version"], new_status, new_data
            )

            self.repository._append_audit(
                connection,
                exception["id"],
                actor.user_id,
                actor.role,
                "confirm",
                current["status"],
                new_status,
                {"confirmations": len(new_confirmations)},
            )
            return updated

        return self.repository.transact(work)

    def _action_release(self, actor, exception, data, expected_version):
        self.rules.check_role(actor, "deviation_exception", "release")

        def work(connection):
            current = self.repository._get_entity(connection, exception["id"])
            business_conflicts = []
            if current["status"] != "confirmed":
                business_conflicts.append(
                    "cannot release: status is %s (two confirmations required)" % current["status"]
                )
            batches = []
            for batch_id in current["data"].get("result_batch_ids") or []:
                batch = self.repository._get_entity(connection, batch_id)
                if not batch:
                    business_conflicts.append("result batch %s not found" % batch_id)
                elif batch["status"] != "frozen":
                    business_conflicts.append(
                        "result batch %s is %s, not frozen" % (batch_id, batch["status"])
                    )
                else:
                    batches.append(batch)
            version_conflict = (
                expected_version is not None and int(expected_version) != current["version"]
            )
            if version_conflict:
                conflicts = [
                    "version conflict: expected %s, found %s"
                    % (expected_version, current["version"])
                ] + business_conflicts
                raise ConcurrentModification("release conflict", latest=current, conflicts=conflicts)
            if business_conflicts:
                raise ConcurrentModification(
                    "release conflict", latest=current, conflicts=business_conflicts
                )

            new_data = dict(current["data"])
            new_data["released_by"] = actor.user_id
            new_data["released_at"] = utcnow()
            updated = self.repository._update_entity(
                connection, exception["id"], current["version"], "released", new_data
            )

            # Unfreeze the linked batches atomically with the exception release.
            for batch in batches:
                batch_data = dict(batch["data"])
                batch_data["released_by_exception"] = exception["id"]
                self.repository._update_entity(
                    connection, batch["id"], batch["version"], "released", batch_data
                )
                self.repository._append_audit(
                    connection,
                    batch["id"],
                    actor.user_id,
                    actor.role,
                    "release",
                    "frozen",
                    "released",
                    {"exception_id": exception["id"]},
                )
            self.repository._append_audit(
                connection,
                exception["id"],
                actor.user_id,
                actor.role,
                "release",
                current["status"],
                "released",
                {"released_batches": [batch["id"] for batch in batches]},
            )
            return updated

        return self.repository.transact(work)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
