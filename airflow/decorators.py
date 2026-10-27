"""Airflow Decorators Shim for Local IDE Resolution"""
from typing import Any, Callable

def dag(*args: Any, **kwargs: Any) -> Callable:
    def decorator(f: Callable) -> Callable:
        def dag_builder(*d_args: Any, **d_kwargs: Any) -> Any:
            try:
                return f(*d_args, **d_kwargs)
            except Exception:
                return None
        return dag_builder
    return decorator

def task(*args: Any, **kwargs: Any) -> Callable:
    def decorator(f: Callable) -> Callable:
        def task_mock(*t_args: Any, **t_kwargs: Any) -> Any:
            # Inside DAG definition, Airflow returns a Task representation, not eager execution
            return f"XComArg_{f.__name__}"
        return task_mock
    return decorator
