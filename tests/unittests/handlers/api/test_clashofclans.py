import unittest.mock as mock

import pytest
import requests

from handlers.api.clashofclans import ClashOfClansHandler


@pytest.fixture
def mock_token_env(monkeypatch):
    """
    Fixture to set environment variable for Clash of Clans API Token.
    """
    mock_env_var = "CLASHOFCLANS_API_TOKEN"
    monkeypatch.setenv(mock_env_var, "my-secret-token")
    return mock_env_var


def test_clashofclans_handler_get_data(mock_token_env):
    """
    Test ClashOfClansHandler.get_data_iter() using unittest.mock.
    """
    # Initialize the handler with the token environment variable name
    handler = ClashOfClansHandler(token_env_var=mock_token_env)

    # Fake API response (mocking requests.get)
    fake_response_data = {
        "tag": "#JU1PVXXX",
        "name": "PLAYER1",
        "attackWins": 123,
        "defenseWins": 99,
    }

    with mock.patch.object(requests, "get") as mock_get:
        # Configure the mock to return a fake response
        mock_response = mock.Mock()
        mock_response.json.return_value = fake_response_data
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Call the method under test with required parameters for achievements, troops, and heroes
        all_chunks = list(handler.get_data_iter(
            player_tag="#JU1PVXXX",
            player_achievements="achievements",
            player_troops="troops",
            player_heroes="heroes"
        ))

        # Ensure the returned data is structured as expected:
        # all_chunks is a list of chunks (each chunk is a list of dictionaries)
        assert isinstance(all_chunks, list)
        assert len(all_chunks) > 0

        # Extract the first chunk (which is a list) and then the first record (dictionary)
        first_chunk = all_chunks[0]
        assert isinstance(first_chunk, list)
        assert len(first_chunk) > 0
        first_record = first_chunk[0]

        # Check the expected fields: note that "id" should have the '#' stripped.
        assert first_record["id"] == "JU1PVXXX"
        assert first_record["name"] == "PLAYER1"
        assert first_record["attackWins"] == 123
        assert first_record["defenseWins"] == 99

        # The additional fields should be JSON-encoded empty lists, as the fake payload has no data for them.
        assert first_record["achievements"] == "[]"
        assert first_record["troops"] == "[]"
        assert first_record["heroes"] == "[]"

        # Verify that last_mtime is present and is a non-empty string.
        assert "last_mtime" in first_record
        assert isinstance(first_record["last_mtime"], str)
        assert first_record["last_mtime"]

        # Confirm that requests.get was called exactly once.
        mock_get.assert_called_once()
