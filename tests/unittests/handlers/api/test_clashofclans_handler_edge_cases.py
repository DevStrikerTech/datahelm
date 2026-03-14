import unittest.mock as mock

import pytest
import requests

from handlers.api.clashofclans import ClashOfClansHandler


def test_handler_raises_when_token_missing(monkeypatch):
    monkeypatch.delenv("CLASHOFCLANS_API_TOKEN", raising=False)
    handler = ClashOfClansHandler(token_env_var="CLASHOFCLANS_API_TOKEN")

    with pytest.raises(EnvironmentError, match="is not set or empty"):
        list(
            handler.get_data_iter(
                player_tag="#ABC",
                player_achievements="achievements",
                player_troops="troops",
                player_heroes="heroes",
            )
        )


def test_handler_raises_on_non_200_response(monkeypatch):
    monkeypatch.setenv("CLASHOFCLANS_API_TOKEN", "token")
    handler = ClashOfClansHandler(token_env_var="CLASHOFCLANS_API_TOKEN")

    with mock.patch.object(requests, "get") as mock_get:
        response = mock.Mock()
        response.status_code = 500
        response.text = "internal error"
        mock_get.return_value = response

        with pytest.raises(RuntimeError, match="HTTP 500"):
            list(
                handler.get_data_iter(
                    player_tag="#ABC",
                    player_achievements="achievements",
                    player_troops="troops",
                    player_heroes="heroes",
                )
            )


def test_handler_propagates_request_exception(monkeypatch):
    monkeypatch.setenv("CLASHOFCLANS_API_TOKEN", "token")
    handler = ClashOfClansHandler(token_env_var="CLASHOFCLANS_API_TOKEN")

    with mock.patch.object(requests, "get", side_effect=requests.RequestException("network")):
        with pytest.raises(requests.RequestException, match="network"):
            list(
                handler.get_data_iter(
                    player_tag="#ABC",
                    player_achievements="achievements",
                    player_troops="troops",
                    player_heroes="heroes",
                )
            )


def test_handler_encodes_tag_and_sets_bearer_header(monkeypatch):
    monkeypatch.setenv("CLASHOFCLANS_API_TOKEN", "token-123")
    handler = ClashOfClansHandler(token_env_var="CLASHOFCLANS_API_TOKEN")

    with mock.patch.object(requests, "get") as mock_get:
        response = mock.Mock()
        response.status_code = 200
        response.json.return_value = {
            "tag": "#ABC",
            "name": "NISH",
            "attackWins": 1,
            "defenseWins": 2,
        }
        mock_get.return_value = response

        rows = list(
            handler.get_data_iter(
                player_tag="#ABC",
                player_achievements="achievements",
                player_troops="troops",
                player_heroes="heroes",
            )
        )

        assert len(rows) == 1
        assert rows[0][0]["id"] == "ABC"
        assert rows[0][0]["last_mtime"]

        called_url = mock_get.call_args.args[0]
        called_headers = mock_get.call_args.kwargs["headers"]
        assert "%23ABC" in called_url
        assert called_headers["Authorization"] == "Bearer token-123"
