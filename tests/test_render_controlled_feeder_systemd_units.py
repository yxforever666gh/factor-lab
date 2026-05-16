from __future__ import annotations

from pathlib import Path

from scripts.render_controlled_feeder_systemd_units import render_units, write_units


def test_render_units_uses_config_file_and_conservative_timer():
    units = render_units()

    assert "--write --config configs/controlled_admission_feeder.json" in units["service"]
    assert "OnUnitActiveSec=30min" in units["timer"]
    assert "AccuracySec=1min" in units["timer"]
    assert "systemctl" not in units["service"]
    assert "systemctl" not in units["timer"]


def test_write_units_writes_preview_files_only(tmp_path):
    result = write_units(tmp_path)

    service = Path(result["service_path"])
    timer = Path(result["timer_path"])
    assert service.exists()
    assert timer.exists()
    assert service.parent == tmp_path
    assert timer.parent == tmp_path
    assert "factor-lab-controlled-admission-feeder.service" == service.name
