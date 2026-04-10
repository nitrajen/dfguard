"""arm() warns when called from __main__."""

import inspect
import warnings


def test_arm_warns_in_main(monkeypatch):
    import frameguard.pyspark._enforcement as _e

    # Simulate __main__ context by patching inspect.currentframe
    fake_frame_globals = {"__package__": None, "__name__": "__main__"}

    class FakeInnerFrame:
        f_globals = fake_frame_globals

    class FakeOuterFrame:
        f_back = FakeInnerFrame()

    monkeypatch.setattr(inspect, "currentframe", lambda: FakeOuterFrame())

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _e.arm()
        assert len(w) == 1
        assert "enforce" in str(w[0].message).lower()
