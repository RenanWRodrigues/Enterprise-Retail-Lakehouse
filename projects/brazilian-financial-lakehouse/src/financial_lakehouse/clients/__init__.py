"""API clients (HTTP only): no storage, no business logic."""

from financial_lakehouse.clients.base import RawPage, ResponseSchemaError
from financial_lakehouse.clients.bcb_ptax import PtaxClient
from financial_lakehouse.clients.bcb_sgs import SgsClient, date_windows
from financial_lakehouse.clients.http import HttpClient, RetryPolicy
from financial_lakehouse.clients.ibge import IbgeAgregadosClient

__all__ = [
    "HttpClient",
    "IbgeAgregadosClient",
    "PtaxClient",
    "RawPage",
    "ResponseSchemaError",
    "RetryPolicy",
    "SgsClient",
    "date_windows",
]
