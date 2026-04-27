"""Phase 2 tests for the admin approval workflow endpoints.

Covers ``GET /api/admin/approvals``, ``POST /api/admin/approvals/{id}/approve``,
and ``POST /api/admin/approvals/{id}/reject``.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from .conftest import make_db_row, valid_config_payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pending_audit_row(
    pending_edit_id: str,
    *,
    dr_id: str = "DR-1042",
    requester: str = "alice@co.com",
    performed_at: str = "2026-04-20T10:00:00+00:00",
    changes: list[dict] | None = None,
    proposed_config_json: str | None = None,
) -> dict:
    """Build a CONFIG_EDIT_PENDING audit row dict matching the schema written
    by :func:`backend.approvals.stage_pending_edit`."""
    if changes is None:
        changes = [
            {"field": "access.developers", "before": ["alice@co.com"],
             "after": ["alice@co.com", "bob@co.com"]}
        ]
    if proposed_config_json is None:
        proposed_config_json = json.dumps(
            valid_config_payload(
                dr_id=dr_id, developers=["alice@co.com", "bob@co.com"]
            )
        )
    detail = {
        "pending_edit_id": pending_edit_id,
        "changes": changes,
        "proposed_config_json": proposed_config_json,
    }
    return {
        "dr_id": dr_id,
        "action": "CONFIG_EDIT_PENDING",
        "performed_by": requester,
        "performed_at": performed_at,
        "status": "PENDING",
        "action_detail": json.dumps(detail),
        "error_message": None,
    }


def _resolved_audit_row(
    pending_edit_id: str,
    *,
    action: str,
    dr_id: str = "DR-1042",
    performed_at: str = "2026-04-21T10:00:00+00:00",
) -> dict:
    """Build a CONFIG_EDIT_APPROVED / CONFIG_EDIT_REJECTED audit row."""
    return {
        "dr_id": dr_id,
        "action": action,
        "performed_by": "admin@co.com",
        "performed_at": performed_at,
        "status": "SUCCESS" if action == "CONFIG_EDIT_APPROVED" else "REJECTED",
        "action_detail": json.dumps({"pending_edit_id": pending_edit_id}),
        "error_message": None,
    }


def _patch_admin_repos(audit_rows: dict[str, list[dict]]):
    """Build a ``patch(...)`` context for ``backend.router_admin._control_repos``.

    *audit_rows* maps action -> list of rows to be returned by
    ``audit_repo.list_by_action(action=<action>)``.

    Returns ``(ctx, mock_dr_repo, mock_obj_repo, mock_access_repo, mock_audit_repo)``.
    """
    mock_dr = MagicMock()
    mock_obj = MagicMock()
    mock_access = MagicMock()
    mock_audit = MagicMock()
    mock_audit.list_by_action = MagicMock(
        side_effect=lambda _db, action: list(audit_rows.get(action, []))
    )
    ctx = patch(
        "backend.router_admin._control_repos",
        return_value=(mock_dr, mock_obj, mock_access, mock_audit),
    )
    return ctx, mock_dr, mock_obj, mock_access, mock_audit


# ---------------------------------------------------------------------------
# GET /api/admin/approvals
# ---------------------------------------------------------------------------


class TestListApprovals:
    def test_lists_pending_only(self, client, mock_db):
        """Pending rows whose pending_edit_id appears in an APPROVED twin
        are filtered out; rejected list is empty in this scenario."""
        pe_a = "pe-aaa111111111"
        pe_b = "pe-bbb222222222"
        pe_c = "pe-ccc333333333"
        rows = {
            "CONFIG_EDIT_PENDING": [
                _pending_audit_row(pe_a, requester="alice@co.com"),
                _pending_audit_row(pe_b, requester="bob@co.com"),
                _pending_audit_row(pe_c, requester="carol@co.com"),
            ],
            "CONFIG_EDIT_APPROVED": [
                _resolved_audit_row(pe_b, action="CONFIG_EDIT_APPROVED"),
            ],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, *_ = _patch_admin_repos(rows)
        with ctx:
            resp = client.get("/api/admin/approvals")

        assert resp.status_code == 200
        data = resp.json()
        # 2 pending edits + 0 pending provisions (mock_db.sql returns [] by default)
        assert data["total"] == 2
        ids = [item["pending_edit_id"] for item in data["pending"]]
        assert set(ids) == {pe_a, pe_c}
        # Each item exposes parsed changes.
        for item in data["pending"]:
            assert isinstance(item["changes"], list)
            assert item["changes"][0]["field"] == "access.developers"
        # New: pending_provisions key always present (Option A unified queue)
        assert data["pending_provisions"] == []

    def test_empty(self, client, mock_db):
        """No pending/approved/rejected rows -> empty list."""
        ctx, *_ = _patch_admin_repos({
            "CONFIG_EDIT_PENDING": [],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        })
        with ctx:
            resp = client.get("/api/admin/approvals")

        assert resp.status_code == 200
        assert resp.json() == {"pending": [], "pending_provisions": [], "total": 0}

    def test_requires_admin(self, user_client, mock_db):
        """Non-admin caller gets 403."""
        resp = user_client.get("/api/admin/approvals")
        assert resp.status_code == 403
        assert resp.json()["detail"] == "Admin access required."

    def test_lists_pending_provisions(self, client, mock_db):
        """Configs with status='scanned' surface in pending_provisions."""
        scanned_row = {
            "dr_id": "DR-2042",
            "config_json": "{}",
            "config_yaml": "",
            "status": "scanned",
            "validation_errors": "[]",
            "created_at": "2026-04-15T00:00:00+00:00",
            "created_by": "alice@co.com",
            "updated_at": None,
            "expiration_date": "2026-08-01",
            "description": "scanned & awaiting provision",
            "manifest_json": json.dumps({
                "scan_result": {
                    "total_objects": 7,
                    "schemas_required": ["dev.s1", "dev.s2"],
                    "review_required": True,
                    "non_prod_additional_objects": ["dev_analytics.scratch.foo"],
                }
            }),
            "scanned_at": "2026-04-15T01:00:00+00:00",
        }
        # mix in an unrelated 'valid' row to confirm filtering
        valid_row = {
            "dr_id": "DR-2099", "config_json": "{}", "config_yaml": "",
            "status": "valid", "validation_errors": "[]",
            "created_at": "2026-04-15T00:00:00+00:00",
            "created_by": "bob@co.com", "updated_at": None,
            "expiration_date": "2026-08-01", "description": None,
            "manifest_json": None, "scanned_at": None,
        }
        mock_db.sql.return_value = [scanned_row, valid_row]
        # No pending edits.
        ctx, *_ = _patch_admin_repos({
            "CONFIG_EDIT_PENDING": [],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        })
        with ctx:
            resp = client.get("/api/admin/approvals")

        assert resp.status_code == 200
        data = resp.json()
        assert data["pending"] == []
        assert len(data["pending_provisions"]) == 1
        prov = data["pending_provisions"][0]
        assert prov["dr_id"] == "DR-2042"
        assert prov["requested_by"] == "alice@co.com"
        assert prov["scanned_at"] == "2026-04-15T01:00:00+00:00"
        assert prov["total_objects"] == 7
        assert prov["total_schemas"] == 2
        assert prov["review_required"] is True
        assert prov["non_prod_additional_objects"] == ["dev_analytics.scratch.foo"]
        assert data["total"] == 1


# ---------------------------------------------------------------------------
# POST /api/admin/approvals/{id}/approve
# ---------------------------------------------------------------------------


class TestApproveEdit:
    @staticmethod
    def _existing_provisioned_row(developers):
        """Build the row returned by config_repo.get for the approve flow."""
        cfg = valid_config_payload(dr_id="DR-1042", developers=developers)
        row = make_db_row(dr_id="DR-1042", status="provisioned")
        row["config_json"] = json.dumps(cfg)
        return row

    def test_approve_applies_diff_and_grants(self, client, mock_db):
        """Happy path: pending row exists, current config has only alice;
        approving should add bob, call _manage_users(add_users, dev), and
        write a CONFIG_EDIT_APPROVED audit row."""
        pe_id = "pe-approve000001"
        proposed = json.dumps(
            valid_config_payload(
                dr_id="DR-1042", developers=["alice@co.com", "bob@co.com"]
            )
        )
        rows = {
            "CONFIG_EDIT_PENDING": [
                _pending_audit_row(pe_id, proposed_config_json=proposed),
            ],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, _dr, _obj, _access, mock_audit = _patch_admin_repos(rows)

        # Existing config row (alice only)
        existing = self._existing_provisioned_row(["alice@co.com"])

        # Patch _get_repo to return a fake repo that surfaces the existing row.
        fake_config_repo = MagicMock()
        fake_config_repo.get.return_value = existing
        fake_config_repo.update = MagicMock()

        with ctx, \
             patch("backend.router_admin._get_repo", return_value=fake_config_repo), \
             patch("devmirror.modify.modification_engine._manage_users") as mock_manage:
            resp = client.post(f"/api/admin/approvals/{pe_id}/approve")

        assert resp.status_code == 200
        body = resp.json()
        assert body["pending_edit_id"] == pe_id
        assert body["status"] == "approved"
        assert "message" in body

        # repo.update was called with the new config (containing bob).
        assert fake_config_repo.update.called
        update_kwargs = fake_config_repo.update.call_args.kwargs
        assert update_kwargs["dr_id"] == "DR-1042"
        new_cfg = json.loads(update_kwargs["config_json"])
        assert "bob@co.com" in new_cfg["developers"]

        # _manage_users called for add of bob.
        # Called positionally:
        # ("add_users", dr_id, ["bob@co.com"], "dev", db_client, obj_repo, access_repo)
        add_calls = [c for c in mock_manage.call_args_list if c.args[0] == "add_users"]
        assert any(
            c.args[1] == "DR-1042" and c.args[2] == ["bob@co.com"] and c.args[3] == "dev"
            for c in add_calls
        )

        # CONFIG_EDIT_APPROVED audit row appended with SUCCESS + pending_edit_id.
        approved_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_APPROVED"
        ]
        assert len(approved_calls) == 1
        approved_kwargs = approved_calls[0].kwargs
        assert approved_kwargs["status"] == "SUCCESS"
        approved_detail = json.loads(approved_kwargs["action_detail"])
        assert approved_detail["pending_edit_id"] == pe_id

    def test_approve_pending_not_found(self, client, mock_db):
        """No matching pending edit -> 404."""
        ctx, *_ = _patch_admin_repos({
            "CONFIG_EDIT_PENDING": [],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        })
        with ctx:
            resp = client.post("/api/admin/approvals/pe-doesnotexist/approve")
        assert resp.status_code == 404

    def test_approve_already_resolved(self, client, mock_db):
        """A pending row that has been APPROVED previously is treated as not-found."""
        pe_id = "pe-resolved00001"
        rows = {
            "CONFIG_EDIT_PENDING": [_pending_audit_row(pe_id)],
            "CONFIG_EDIT_APPROVED": [
                _resolved_audit_row(pe_id, action="CONFIG_EDIT_APPROVED"),
            ],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, *_ = _patch_admin_repos(rows)
        with ctx:
            resp = client.post(f"/api/admin/approvals/{pe_id}/approve")
        assert resp.status_code == 404

    def test_approve_with_grant_failure_returns_partial(self, client, mock_db):
        """If _manage_users raises, the response should be 200 with
        ``status="partial"`` and an audit row with status ``"PARTIAL"``."""
        pe_id = "pe-partial000001"
        proposed = json.dumps(
            valid_config_payload(
                dr_id="DR-1042", developers=["alice@co.com", "bob@co.com"]
            )
        )
        rows = {
            "CONFIG_EDIT_PENDING": [
                _pending_audit_row(pe_id, proposed_config_json=proposed),
            ],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, _dr, _obj, _access, mock_audit = _patch_admin_repos(rows)

        existing = self._existing_provisioned_row(["alice@co.com"])
        fake_config_repo = MagicMock()
        fake_config_repo.get.return_value = existing

        with ctx, \
             patch("backend.router_admin._get_repo", return_value=fake_config_repo), \
             patch(
                 "devmirror.modify.modification_engine._manage_users",
                 side_effect=RuntimeError("grant failed"),
             ):
            resp = client.post(f"/api/admin/approvals/{pe_id}/approve")

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "partial"
        assert body["pending_edit_id"] == pe_id

        partial_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_APPROVED"
        ]
        assert len(partial_calls) == 1
        assert partial_calls[0].kwargs["status"] == "PARTIAL"

    def test_approve_requires_admin(self, user_client, mock_db):
        """Non-admin caller gets 403."""
        resp = user_client.post("/api/admin/approvals/pe-anything/approve")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /api/admin/approvals/{id}/reject
# ---------------------------------------------------------------------------


class TestRejectEdit:
    def test_reject_writes_rejected_audit(self, client, mock_db):
        """Reject with a reason -> CONFIG_EDIT_REJECTED audit row, no
        config update, response 200/rejected."""
        pe_id = "pe-reject0000001"
        rows = {
            "CONFIG_EDIT_PENDING": [_pending_audit_row(pe_id)],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, _dr, _obj, _access, mock_audit = _patch_admin_repos(rows)

        # Provide a fake config repo so we can assert update was NOT called.
        fake_config_repo = MagicMock()
        fake_config_repo.get.return_value = make_db_row(status="provisioned")

        with ctx, patch("backend.router_admin._get_repo", return_value=fake_config_repo):
            resp = client.post(
                f"/api/admin/approvals/{pe_id}/reject",
                json={"reason": "ops blocker"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["pending_edit_id"] == pe_id
        assert body["status"] == "rejected"

        rejected_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_REJECTED"
        ]
        assert len(rejected_calls) == 1
        kwargs = rejected_calls[0].kwargs
        assert kwargs["status"] == "REJECTED"
        detail = json.loads(kwargs["action_detail"])
        assert detail["pending_edit_id"] == pe_id
        assert detail["reason"] == "ops blocker"

        # repo.update must NOT be called on reject.
        fake_config_repo.update.assert_not_called()

    def test_reject_no_reason(self, client, mock_db):
        """Body omits reason -> audit row written with empty reason."""
        pe_id = "pe-noreason00001"
        rows = {
            "CONFIG_EDIT_PENDING": [_pending_audit_row(pe_id)],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        }
        ctx, _dr, _obj, _access, mock_audit = _patch_admin_repos(rows)
        with ctx:
            # Send empty body (no JSON content).
            resp = client.post(f"/api/admin/approvals/{pe_id}/reject")

        assert resp.status_code == 200
        rejected = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_REJECTED"
        ]
        assert len(rejected) == 1
        detail = json.loads(rejected[0].kwargs["action_detail"])
        assert detail["pending_edit_id"] == pe_id
        # reason absent -> empty string (treated as falsy null by the router).
        assert detail.get("reason", "") in ("", None)

    def test_reject_pending_not_found(self, client, mock_db):
        """No matching pending edit -> 404."""
        ctx, *_ = _patch_admin_repos({
            "CONFIG_EDIT_PENDING": [],
            "CONFIG_EDIT_APPROVED": [],
            "CONFIG_EDIT_REJECTED": [],
        })
        with ctx:
            resp = client.post(
                "/api/admin/approvals/pe-missing/reject",
                json={"reason": "n/a"},
            )
        assert resp.status_code == 404

    def test_reject_requires_admin(self, user_client, mock_db):
        """Non-admin caller gets 403."""
        resp = user_client.post(
            "/api/admin/approvals/pe-anything/reject",
            json={"reason": "no"},
        )
        assert resp.status_code == 403
