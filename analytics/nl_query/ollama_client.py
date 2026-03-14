from __future__ import annotations

from typing import Any, Dict

import requests


class OllamaClient:
    """
    Minimal client for local Ollama generate endpoint.
    """

    def __init__(self, model: str = "llama3.1:8b", base_url: str = "http://localhost:11434", timeout: int = 90):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def generate(self, prompt: str, system_prompt: str = "") -> str:
        payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
        }
        if system_prompt:
            payload["system"] = system_prompt

        response = requests.post(
            f"{self.base_url}/api/generate",
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.json()
        return str(body.get("response", "")).strip()
