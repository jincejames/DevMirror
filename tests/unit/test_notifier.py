"""Unit tests for devmirror.cleanup.notifier."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from devmirror.cleanup.notifier import (
    LoggingBackend,
    NotificationBackend,
    NotificationContent,
    _extract_recipients,
    build_notification,
    find_drs_needing_notification,
    notify_expiring_drs,
)

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

_DR_ROW: dict[str, Any] = {
    "dr_id": "DR-100", "description": "Test pipeline DR",
    "expiration_date": "2026-04-20", "status": "ACTIVE",
    "notification_sent_at": None, "created_by": "admin@co.com", "config_yaml": None,
}


class _FakeBackend:
    def __init__(self, ok: bool = True) -> None:
        self.sent: list[NotificationContent] = []
        self._ok = ok

    def send(self, n: NotificationContent) -> bool:
        self.sent.append(n)
        return self._ok


def _mock_db(rows=None) -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql_exec_with_params = MagicMock()
    m.sql = MagicMock(return_value=rows or [])
    # Wire sql_with_params to delegate to sql so existing return_value/side_effect work
    m.sql_with_params.side_effect = lambda stmt, params: m.sql(stmt, params)
    return m


def _repos():
    db = _mock_db()
    dr = MagicMock()
    dr.table_fqn = "ctl.admin.devmirror_development_requests"
    dr.update_notification_sent = MagicMock()
    obj = MagicMock()
    obj.list_by_dr_id = MagicMock(return_value=[{"dr_id": "DR-100", "source_fqn": "p.s.t"}])
    aud = MagicMock()
    aud.append = MagicMock()
    return db, dr, obj, aud


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

class TestBuildNotification:
    def test_builds_content(self) -> None:
        n = build_notification(_DR_ROW, 5, ["dev@co.com"])
        assert n.dr_id == "DR-100"
        assert "DR-100" in n.subject and "5" in n.body

    def test_empty_description(self) -> None:
        row = {**_DR_ROW, "description": None}
        assert build_notification(row, 0, []).description == ""


class TestLoggingBackend:
    def test_protocol_compliant_and_returns_true(self) -> None:
        b = LoggingBackend()
        assert isinstance(b, NotificationBackend)
        n = NotificationContent(dr_id="DR-1", description="", expiration_date="x",
                                object_count=0, recipients=[], subject="s", body="b")
        assert b.send(n) is True


class TestFindDrs:
    def test_query_criteria(self) -> None:
        db, dr, *_ = _repos()
        find_drs_needing_notification(db, dr, notification_days=14)
        q, params = db.sql_with_params.call_args[0]
        assert "notification_sent_at IS NULL" in q and "14" in q
        assert params == {"status": "ACTIVE"}

    def test_params_shape(self) -> None:
        db, dr, *_ = _repos()
        find_drs_needing_notification(db, dr, notification_days=7)
        q, params = db.sql_with_params.call_args[0]
        assert "DATE_SUB(expiration_date, 7)" in q
        assert params == {"status": "ACTIVE"}


class TestNotifyExpiringDrs:
    def test_no_drs(self) -> None:
        db, dr, obj, aud = _repos()
        b = _FakeBackend()
        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, backend=b)
        assert r.notified == 0

    def test_success(self) -> None:
        db, dr, obj, aud = _repos()
        db.sql.return_value = [_DR_ROW]
        b = _FakeBackend()
        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, backend=b)
        assert r.notified == 1 and b.sent[0].dr_id == "DR-100"
        dr.update_notification_sent.assert_called_once()
        aud.append.assert_called_once()

    def test_double_send_guard(self) -> None:
        db, dr, obj, aud = _repos()
        db.sql.return_value = [{**_DR_ROW, "notification_sent_at": "2026-04-13T08:00:00+00:00"}]
        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, backend=_FakeBackend())
        assert r.skipped == 1

    def test_backend_failure(self) -> None:
        db, dr, obj, aud = _repos()
        db.sql.return_value = [_DR_ROW]
        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, backend=_FakeBackend(ok=False))
        assert len(r.failed) == 1

    def test_backend_exception(self) -> None:
        db, dr, obj, aud = _repos()
        db.sql.return_value = [_DR_ROW]

        class Boom:
            def send(self, n):
                raise RuntimeError("SMTP down")

        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, backend=Boom())
        assert "SMTP down" in r.failed[0][1]

    def test_defaults_to_logging(self) -> None:
        db, dr, obj, aud = _repos()
        db.sql.return_value = [_DR_ROW]
        r = notify_expiring_drs(db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud)
        assert r.notified == 1


class TestExtractRecipients:
    def test_from_notification_recipients(self) -> None:
        import yaml
        cfg = {"development_request": {"lifecycle": {"notification_recipients": ["a@co.com"]}, "access": {"developers": ["b@co.com"]}}}
        assert _extract_recipients({"config_yaml": yaml.dump(cfg), "created_by": "sys"}) == ["a@co.com"]

    def test_fallback_to_developers(self) -> None:
        import yaml
        cfg = {"development_request": {"lifecycle": {}, "access": {"developers": ["b@co.com"]}}}
        assert _extract_recipients({"config_yaml": yaml.dump(cfg), "created_by": "sys"}) == ["b@co.com"]

    def test_fallback_to_created_by(self) -> None:
        assert _extract_recipients({"config_yaml": None, "created_by": "a@co.com"}) == ["a@co.com"]

    def test_invalid_yaml(self) -> None:
        assert _extract_recipients({"config_yaml": "::bad::", "created_by": "a@co.com"}) == ["a@co.com"]
