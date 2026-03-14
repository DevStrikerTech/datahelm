import subprocess
from pathlib import Path

import pytest

from analytics.dbt_runner import _append_multi_value_flag, run_dbt_command


def test_append_multi_value_flag_adds_repeated_flag_and_values():
    command = ["dbt", "build"]

    _append_multi_value_flag(command, "--select", ["tag:a", "path:models/staging"])

    assert command == [
        "dbt",
        "build",
        "--select",
        "tag:a",
        "--select",
        "path:models/staging",
    ]


def test_run_dbt_command_builds_expected_cli(monkeypatch, tmp_path):
    captured = {}
    project_dir = tmp_path / "project"
    profiles_dir = tmp_path / "profiles"
    project_dir.mkdir()
    profiles_dir.mkdir()

    def fake_run(command, check, text, capture_output):
        captured["command"] = command
        captured["check"] = check
        captured["text"] = text
        captured["capture_output"] = capture_output
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr("analytics.dbt_runner.subprocess.run", fake_run)

    run_dbt_command(
        project_name="CLASHOFCLANS_SOURCE_STAGING",
        project_config={
            "project_dir": str(project_dir),
            "profiles_dir": str(profiles_dir),
            "profile_name": "datahelm",
            "target": "dev",
            "select": ["tag:clashofclans", "path:models/staging"],
            "exclude": ["path:models/marts"],
            "vars": {"target_schema": "clashofclans_analytics"},
        },
        command_name="build",
    )

    assert captured["check"] is False
    assert captured["text"] is True
    assert captured["capture_output"] is True

    command = captured["command"]
    assert command[0:2] == ["dbt", "build"]
    assert "--project-dir" in command
    assert str(Path(project_dir).resolve()) in command
    assert "--profiles-dir" in command
    assert str(Path(profiles_dir).resolve()) in command
    assert command.count("--select") == 2
    assert command.count("--exclude") == 1
    assert "--vars" in command


def test_run_dbt_command_raises_runtime_error_on_non_zero_exit(monkeypatch, tmp_path):
    project_dir = tmp_path / "project"
    profiles_dir = tmp_path / "profiles"
    project_dir.mkdir()
    profiles_dir.mkdir()

    def fake_run(command, check, text, capture_output):
        return subprocess.CompletedProcess(command, 2, stdout="", stderr="boom")

    monkeypatch.setattr("analytics.dbt_runner.subprocess.run", fake_run)

    with pytest.raises(RuntimeError, match="dbt command failed"):
        run_dbt_command(
            project_name="CLASHOFCLANS_SOURCE_MARTS",
            project_config={
                "project_dir": str(project_dir),
                "profiles_dir": str(profiles_dir),
                "profile_name": "datahelm",
                "target": "dev",
            },
            command_name="build",
        )
