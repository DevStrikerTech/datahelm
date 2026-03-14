import sys
import types
import unittest.mock as mock

import pytest
import requests

from handlers.sharepoint import SharePointConfig, SharePointSourceConnector


def test_sharepoint_from_env_maps_values():
    connector = SharePointSourceConnector.from_env(
        {
            "SHAREPOINT_TENANT_ID": "tenant",
            "SHAREPOINT_CLIENT_ID": "client",
            "SHAREPOINT_CLIENT_SECRET": "secret",
            "SHAREPOINT_SITE_HOSTNAME": "acme.sharepoint.com",
            "SHAREPOINT_SITE_PATH": "/sites/data-team",
            "SHAREPOINT_TIMEOUT_SECONDS": "45",
        }
    )

    assert connector.config.tenant_id == "tenant"
    assert connector.config.client_id == "client"
    assert connector.config.client_secret == "secret"
    assert connector.config.site_hostname == "acme.sharepoint.com"
    assert connector.config.site_path == "/sites/data-team"
    assert connector.config.timeout_seconds == 45


def test_sharepoint_authenticate_raises_for_missing_required_values():
    connector = SharePointSourceConnector(
        SharePointConfig(
            tenant_id="",
            client_id="",
            client_secret="",
            site_hostname="",
            site_path="",
        )
    )

    with pytest.raises(ValueError, match="Missing SharePoint configuration values"):
        connector.authenticate()


def test_sharepoint_authenticate_uses_msal_client_credentials(monkeypatch):
    captured = {}

    class _FakeMsalApp:
        def __init__(self, client_id, client_credential, authority):
            captured["client_id"] = client_id
            captured["client_credential"] = client_credential
            captured["authority"] = authority

        def acquire_token_for_client(self, scopes):
            captured["scopes"] = scopes
            return {"access_token": "token-123"}

    fake_msal = types.SimpleNamespace(ConfidentialClientApplication=_FakeMsalApp)
    monkeypatch.setitem(sys.modules, "msal", fake_msal)

    connector = SharePointSourceConnector(
        SharePointConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            client_secret="client-secret",
            site_hostname="acme.sharepoint.com",
            site_path="/sites/data-team",
        )
    )
    token = connector.authenticate()

    assert token == "token-123"
    assert captured["client_id"] == "client-id"
    assert captured["client_credential"] == "client-secret"
    assert captured["authority"].endswith("/tenant-id")
    assert captured["scopes"] == ["https://graph.microsoft.com/.default"]


def test_sharepoint_get_site_id_and_download_file(monkeypatch):
    connector = SharePointSourceConnector(
        SharePointConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            client_secret="client-secret",
            site_hostname="acme.sharepoint.com",
            site_path="/sites/data-team",
        )
    )

    monkeypatch.setattr(connector, "authenticate", lambda: "token-123")

    with mock.patch.object(requests, "get") as mock_get:
        site_response = mock.Mock()
        site_response.status_code = 200
        site_response.json.return_value = {"id": "site-1"}

        file_response = mock.Mock()
        file_response.status_code = 200
        file_response.content = b"payload"

        mock_get.side_effect = [site_response, file_response]

        site_id = connector.get_site_id()
        content = connector.download_file("Shared Documents/test.xlsx")

        assert site_id == "site-1"
        assert content == b"payload"
        assert mock_get.call_count == 2


def test_sharepoint_list_folder_items_raises_on_non_200(monkeypatch):
    connector = SharePointSourceConnector(
        SharePointConfig(
            tenant_id="tenant-id",
            client_id="client-id",
            client_secret="client-secret",
            site_hostname="acme.sharepoint.com",
            site_path="/sites/data-team",
        )
    )

    monkeypatch.setattr(connector, "authenticate", lambda: "token-123")
    monkeypatch.setattr(connector, "get_site_id", lambda refresh=False: "site-1")

    with mock.patch.object(requests, "get") as mock_get:
        response = mock.Mock()
        response.status_code = 500
        mock_get.return_value = response

        with pytest.raises(RuntimeError, match="Failed to list SharePoint folder"):
            connector.list_folder_items("Shared Documents")
