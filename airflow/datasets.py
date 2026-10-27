"""Airflow Datasets Shim for Local IDE Resolution"""
from typing import Any

class Dataset:
    def __init__(self, uri: str, *args: Any, **kwargs: Any):
        self.uri = uri
