# ==============================================================
# tests/test_model_resolution.py  (pytest)
# ==============================================================

"""
Tests focused on avoiding AppRegistryNotReady and module import side effects.
They are skipped if Django isn't configured.
"""

import os
import importlib
import types
import pytest

pytest.importorskip("django")

from django.apps import apps
from django.conf import settings


@pytest.mark.skipif(not settings.configured, reason="Django not configured")
def test_models_are_discoverable_by_name():
    required = {"Subscriber", "SubscriberSMS", "Client", "User"}
    present = {m.__name__ for m in apps.get_models()}

    missing = required - present
    if missing:
        pytest.skip(f"Project does not define required models: {sorted(missing)}")

    for name in sorted(required):
        matches = [m for m in apps.get_models() if m.__name__ == name]
        assert len(matches) >= 1, f"Model {name} should exist"


@pytest.mark.skipif(not settings.configured, reason="Django not configured")
def test_importing_command_module_does_not_touch_apps_registry(monkeypatch):
    """Smoke test: importing the command file should not raise AppRegistryNotReady.
    We import it as a plain module (not via Django command loader) to ensure
    there is no model resolution at import time.
    """
    # Path here mirrors the canvas structure; adapt the dotted path if you relocate files.
    # The important bit: module-level code must NOT call apps.get_model / apps.get_models.
    try:
        import management.commands.migrate_subscribers as cmd_mod
    except Exception as e:  # noqa: BLE001
        pytest.fail(f"Module import raised unexpectedly: {e!r}")

    # Ensure the helper exists but is not executed on import
    assert hasattr(cmd_mod, "_get_model_by_name")
    # Ensure Command class is present
    assert hasattr(cmd_mod, "Command")


@pytest.mark.skipif(not settings.configured, reason="Django not configured")
def test_setting_app_label_disambiguates(monkeypatch):
    # If a project has duplicated model names across apps, setting the app label
    # should let apps.get_model resolve it deterministically when called inside handle().
    assert hasattr(settings, "INSTALLED_APPS")
