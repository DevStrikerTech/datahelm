import pandas as pd

from utils.ingestion_utils import ExtractOutputType, get_extract_output_type


def test_get_extract_output_type_returns_none_for_none_input():
    assert get_extract_output_type(None) is None


def test_get_extract_output_type_identifies_list():
    assert get_extract_output_type([{"id": 1}]) == ExtractOutputType.list_of_dicts


def test_get_extract_output_type_identifies_dataframe():
    df = pd.DataFrame([{"id": 1}])
    assert get_extract_output_type(df) == ExtractOutputType.dataframe
