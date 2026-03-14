import importlib.util
from pathlib import Path


def _load_run_dagster_dev_module():
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "run_dagster_dev.py"
    spec = importlib.util.spec_from_file_location("run_dagster_dev", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolve_dagster_home_prefers_explicit_env(monkeypatch, tmp_path):
    module = _load_run_dagster_dev_module()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    explicit_home = tmp_path / "custom-home"
    monkeypatch.setenv("DAGSTER_HOME", str(explicit_home))
    monkeypatch.setenv("DAGSTER_HOME_DIR", "ignored-home")

    resolved = module._resolve_dagster_home(repo_root)

    assert resolved == explicit_home.resolve()


def test_resolve_dagster_home_uses_repo_relative_default(monkeypatch, tmp_path):
    module = _load_run_dagster_dev_module()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    monkeypatch.setenv("DAGSTER_HOME_DIR", ".dagster_home_custom")

    resolved = module._resolve_dagster_home(repo_root)

    assert resolved == (repo_root / ".dagster_home_custom").resolve()


def test_main_print_only_returns_zero_without_subprocess(monkeypatch):
    module = _load_run_dagster_dev_module()

    monkeypatch.setattr(module, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module.subprocess, "call", lambda *_args, **_kwargs: 99)
    monkeypatch.setattr(module.sys, "argv", ["run_dagster_dev.py", "--print-only"])

    exit_code = module.main()

    assert exit_code == 0
