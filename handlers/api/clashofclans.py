import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Generator, Optional

import requests
from environs import Env

env = Env()
env.read_env()

log = logging.getLogger(__name__)


class ClashOfClansHandler:
    """
    Handler class for retrieving Clash of Clans player data from the official API.
    """

    BASE_URL: str = "https://api.clashofclans.com/v1"

    def __init__(self, token_env_var: str):
        """
        Initialize the ClashOfClansHandler.

        :param token_env_var: The name of the environment variable
            that holds the Clash of Clans API token (Bearer). For example,
            you might set in .env: COC_API_TOKEN_ENV=eyJhbGc...
        """
        self.token_env_var = token_env_var

    def get_data_iter(self, player_tag: str, player_achievements: str, player_troops: str, player_heroes: str,
                      last_mtime: Optional[str] = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generator that fetches player data from Clash of Clans API
        and yields a single list-of-dicts chunk containing the minimal fields.

        :param player_tag: The player tag, e.g. "#CL9LJUUU".
                          If '#' is present, the API typically requires '%23'.
        :yield: A list of dictionaries. Example:
            [
              {
                "tag": "#CL9LJUUU",
                "name": "NISH",
                "attackWins": 0,
                "defenseWins": 0
              }
            ]
        """

        if not last_mtime:
            last_mtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # Retrieve the token from environment
        token_value = env.str(self.token_env_var, default=None)

        if not token_value:
            msg = (
                f"Missing Clash of Clans API token: "
                f"Environment variable '{self.token_env_var}' is not set or empty."
            )
            log.error(msg)
            raise EnvironmentError(msg)

        # Encode the player_tag if needed (# -> %23)
        encoded_tag = player_tag.replace("#", "%23")

        # Construct request
        url = f"{self.BASE_URL}/players/{encoded_tag}"
        headers = {"Authorization": f"Bearer {token_value}"}
        log.info("Requesting Clash of Clans data: %s", url)

        # Perform GET request
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            log.error("Request to Clash of Clans API failed: %s", exc)
            raise

        if response.status_code != 200:
            msg = (
                f"ClashOfClansHandler API request failed. "
                f"HTTP {response.status_code} - {response.text}"
            )
            log.error(msg)
            raise RuntimeError(msg)

        payload = response.json()

        # Extract the minimal fields from the JSON
        record = {
            "id": payload.get("tag").strip("#"),
            "name": payload.get("name"),
            "attackWins": payload.get("attackWins"),
            "defenseWins": payload.get("defenseWins"),
            "achievements": json.dumps(payload.get(player_achievements, [])),
            "troops": json.dumps(payload.get(player_troops, [])),
            "heroes": json.dumps(payload.get(player_heroes, [])),
            "last_mtime": last_mtime
        }

        # Yield a single-chunk list
        yield [record]
