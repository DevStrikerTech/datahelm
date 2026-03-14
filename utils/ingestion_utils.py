from enum import Enum
from typing import Any, Optional

import pandas as pd


class ExtractOutputType(Enum):
    """
    Enum class representing the expected output type of the extraction process.

    Attributes:
        list_of_dicts: Extraction returns a list of dictionaries.
        dataframe: Extraction returns a pandas DataFrame.
    """
    list_of_dicts = 'list_of_dicts'
    dataframe = 'DataFrame'


def get_extract_output_type(inp: Any) -> Optional[ExtractOutputType]:
    """
    Determines the extract output type based on the type of the input data.

    Args:
        inp (Any): The extracted data from an API call or file read.

    Returns:
        Optional[ExtractOutputType]: The corresponding ExtractOutputType if recognized,
            or None if the input is None.
    """
    if inp is None:
        return None

    if isinstance(inp, list):
        return ExtractOutputType.list_of_dicts

    # Attempt to detect pandas DataFrame
    try:
        if isinstance(inp, pd.DataFrame):
            return ExtractOutputType.dataframe
    except ImportError:
        pass
