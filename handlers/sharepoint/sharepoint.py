"""
Reusable SharePoint source connector built on Microsoft Graph.

This connector centralizes authentication and file access so new ingestion
flows can focus on source-specific transformation logic instead of auth/plumbing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)


@dataclass
class SharePointConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    site_hostname: str
    site_path: str
    timeout_seconds: int = 30


class SharePointSourceConnector:
    """
    SharePoint connector that authenticates using Azure app credentials and
    reads file/folder content through Microsoft Graph APIs.
    """

    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, config: SharePointConfig):
        self.config = config
        self._access_token: Optional[str] = None
        self._site_id: Optional[str] = None

    @classmethod
    def from_env(cls, env: Dict[str, str]) -> "SharePointSourceConnector":
        return cls(
            SharePointConfig(
                tenant_id=env.get("SHAREPOINT_TENANT_ID", ""),
                client_id=env.get("SHAREPOINT_CLIENT_ID", ""),
                client_secret=env.get("SHAREPOINT_CLIENT_SECRET", ""),
                site_hostname=env.get("SHAREPOINT_SITE_HOSTNAME", ""),
                site_path=env.get("SHAREPOINT_SITE_PATH", ""),
                timeout_seconds=int(env.get("SHAREPOINT_TIMEOUT_SECONDS", "30")),
            )
        )

    def _validate_required_config(self) -> None:
        missing = []
        if not self.config.tenant_id:
            missing.append("tenant_id")
        if not self.config.client_id:
            missing.append("client_id")
        if not self.config.client_secret:
            missing.append("client_secret")
        if not self.config.site_hostname:
            missing.append("site_hostname")
        if not self.config.site_path:
            missing.append("site_path")

        if missing:
            raise ValueError(f"Missing SharePoint configuration values: {', '.join(missing)}")

    def authenticate(self) -> str:
        """
        Acquire and cache a Microsoft Graph access token using client credentials.
        """
        self._validate_required_config()

        if self._access_token:
            return self._access_token

        try:
            from msal import ConfidentialClientApplication
        except ImportError as exc:
            raise ImportError(
                "msal is required for SharePointSourceConnector. Install with `pip install msal`."
            ) from exc

        authority = f"https://login.microsoftonline.com/{self.config.tenant_id}"
        app = ConfidentialClientApplication(
            client_id=self.config.client_id,
            client_credential=self.config.client_secret,
            authority=authority,
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        token = result.get("access_token")
        if not token:
            description = result.get("error_description", "unknown authentication error")
            raise RuntimeError(f"Failed to authenticate with Microsoft Graph: {description}")

        self._access_token = token
        return token

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.authenticate()}"}

    def get_site_id(self, refresh: bool = False) -> str:
        """
        Resolve and cache SharePoint site id for configured site.
        """
        if self._site_id and not refresh:
            return self._site_id

        site_ref = f"{self.config.site_hostname}:/{self.config.site_path.lstrip('/')}"
        url = f"{self.GRAPH_BASE_URL}/sites/{site_ref}"
        response = requests.get(
            url,
            headers=self._auth_headers(),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to resolve SharePoint site id: HTTP {response.status_code}")

        site_id = response.json().get("id")
        if not site_id:
            raise RuntimeError("Microsoft Graph returned no site id for configured SharePoint site")

        self._site_id = site_id
        return site_id

    def download_file(self, file_path: str) -> bytes:
        """
        Download a file from SharePoint site drive and return raw bytes.
        """
        normalized = file_path.lstrip("/")
        site_id = self.get_site_id()
        url = f"{self.GRAPH_BASE_URL}/sites/{site_id}/drive/root:/{normalized}:/content"
        response = requests.get(
            url,
            headers=self._auth_headers(),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to download SharePoint file '{file_path}': HTTP {response.status_code}"
            )
        return response.content

    def list_folder_items(self, folder_path: str = "", top: int = 200) -> List[Dict[str, Any]]:
        """
        List files/folders from a SharePoint drive folder.
        """
        normalized = folder_path.strip("/")
        site_id = self.get_site_id()
        if normalized:
            url = f"{self.GRAPH_BASE_URL}/sites/{site_id}/drive/root:/{normalized}:/children?$top={top}"
        else:
            url = f"{self.GRAPH_BASE_URL}/sites/{site_id}/drive/root/children?$top={top}"

        response = requests.get(
            url,
            headers=self._auth_headers(),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to list SharePoint folder '{folder_path or '/'}': HTTP {response.status_code}"
            )

        items = response.json().get("value", [])
        log.info("SharePoint list_folder_items returned %d item(s) for path '%s'", len(items), folder_path)
        return items
