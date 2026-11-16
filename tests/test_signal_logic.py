"""Unit tests for the kill-path branching and pre-cancel short-circuit.

Real signal delivery is exercised by the ``@requires_signals`` tests inside
Docker; the hardened CI sandbox denies it, so here the process interactions
are replaced with fakes to verify the surrounding decision logic.
"""
import threading
from unittest import mock

import app.trial as trial_mod
from app.trial import TrialResult


class FakeProc:
    def __init__(self):
        self.pid = 12345
        self._rc = None
        self.stdin = mock.Mock()
        self.stdout = mock.Mock()

    def poll(self):
        return self._rc

    def wait(self, timeout=None):
        self._rc = -9
        return self._rc

    def kill(self):
        self._rc = -9


def test_kill_group_success_path():
    proc = FakeProc()
    with mock.patch("app.trial.os.killpg") as mkill, \
         mock.patch("app.trial.os.getpgid", return_value=proc.pid):
        ok = trial_mod._kill_group(proc)
    assert ok is True
    mkill.assert_called_once()


def test_kill_group_falls_back_to_proc_kill():
    proc = FakeProc()
    with mock.patch("app.trial.os.killpg", side_effect=PermissionError), \
         mock.patch("app.trial.os.getpgid", return_value=proc.pid):
        ok = trial_mod._kill_group(proc)
    assert ok is True


def test_kill_group_denied_returns_false():
    proc = FakeProc()
    with mock.patch("app.trial.os.killpg", side_effect=PermissionError), \
         mock.patch("app.trial.os.getpgid", return_value=proc.pid), \
         mock.patch.object(FakeProc, "kill", side_effect=PermissionError):
        ok = trial_mod._kill_group(proc)
    assert ok is False


def test_run_trial_short_circuits_when_precanceled():
    ev = threading.Event()
    ev.set()
    res = trial_mod.run_trial([["a", "x", "1"]], "SELECT 1", ev)
    assert res.status == "canceled"


def test_trial_result_reproduces_guard():
    ok = TrialResult(status="error", error_type="E", first_line="m")
    assert ok.reproduces is True
    succ = TrialResult(status="success")
    assert succ.reproduces is False
