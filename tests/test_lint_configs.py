import pytest
import subprocess
import os

def test_lint_success():
    # Tests a valid directory (the default 'config' folder)
    result = subprocess.run(["python", "scripts/lint_configs.py", "--path", "config"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Success" in result.stdout

def test_invalid_path():
    # Tests a non-existent directory
    result = subprocess.run(["python", "scripts/lint_configs.py", "--path", "does-not-exist"], capture_output=True, text=True)
    assert result.returncode == 1
    assert "Error: The path" in result.stdout

# You can add more complex tests here later, but this covers the 'fail fast' requirement!