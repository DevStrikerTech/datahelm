from typing import Dict, Any, List, Union

from dagster import get_dagster_logger

log = get_dagster_logger()


class MiscUtils:
    """
    Utility class for general-purpose helper functions.
    """

    @staticmethod
    def normalize_json(
            json_data: Dict[str, Any], parent_key: str = "", separator: str = "_"
    ) -> Dict[str, Any]:
        """
        Recursively flattens a nested JSON structure into a flat dictionary.

        :param json_data: Dict[str, Any] - The raw JSON dictionary.
        :param parent_key: str - Used for recursive key construction.
        :param separator: str - The separator between nested keys (default "_").
        :return: Dict[str, Any] - A flattened dictionary.
        """
        flat_dict = {}

        def _flatten(obj: Union[Dict, List], key_prefix: str = ""):
            """
            Recursive helper function to process the JSON structure.
            """
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{key_prefix}{separator}{key}" if key_prefix else key
                    _flatten(value, new_key)
            elif isinstance(obj, list):
                for index, item in enumerate(obj):
                    new_key = f"{key_prefix}{separator}{index}" if key_prefix else str(index)
                    _flatten(item, new_key)
            else:
                # Base case: store non-dict/list values directly
                flat_dict[key_prefix] = obj

        _flatten(json_data, parent_key)

        return flat_dict
