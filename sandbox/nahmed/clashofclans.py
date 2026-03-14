"""
Example sandbox test script to verify ClashOfClansHandler functionality manually.
"""

from handlers.api.clashofclans import ClashOfClansHandler


def main():
    handler = ClashOfClansHandler(token_env_var="CLASHOFCLANS_API_TOKEN")

    player_tag = "#CL9LJUUU"
    player_achievements = "achievements"
    player_troops = "troops"
    player_heroes = "heroes"

    for chunk in handler.get_data_iter(player_tag=player_tag,
                                       player_achievements=player_achievements,
                                       player_troops=player_troops,
                                       player_heroes=player_heroes):
        print(chunk)


if __name__ == "__main__":
    main()
