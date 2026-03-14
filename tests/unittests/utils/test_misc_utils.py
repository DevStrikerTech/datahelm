from utils.misc_utils import MiscUtils


def test_normalize_json_flattens_nested_dict_and_list():
    payload = {
        "player": {
            "name": "NISH",
            "stats": {
                "attackWins": 10,
            },
        },
        "troops": [
            {"name": "Barbarian"},
            {"name": "Archer"},
        ],
    }

    result = MiscUtils.normalize_json(payload)

    assert result == {
        "player_name": "NISH",
        "player_stats_attackWins": 10,
        "troops_0_name": "Barbarian",
        "troops_1_name": "Archer",
    }


def test_normalize_json_respects_parent_key_and_separator():
    payload = {"a": {"b": 1}}

    result = MiscUtils.normalize_json(payload, parent_key="root", separator=".")

    assert result == {"root.a.b": 1}
