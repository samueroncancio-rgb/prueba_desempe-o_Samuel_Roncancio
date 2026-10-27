import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger("sismowatch.error_handler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

def sanitize_message(msg: str) -> str:
    """Removes sensitive credentials or tokens from error logs."""
    sensitive_keys = ["password", "secret", "token", "key", "pass"]
    sanitized = str(msg)
    for key in sensitive_keys:
        if key in sanitized.lower():
            # Basic redaction to prevent credential leakage
            parts = sanitized.split()
            sanitized = " ".join("[REDACTED]" if any(k in p.lower() for k in sensitive_keys) else p for p in parts)
    return sanitized

def on_failure_alert(context):
    """
    Airflow on_failure_callback.
    Captures context, logs sanitized stacktrace, and emits notification.
    """
    dag_id = context.get("dag", {}).dag_id if hasattr(context.get("dag"), "dag_id") else "UNKNOWN_DAG"
    task_id = context.get("task_instance", {}).task_id if hasattr(context.get("task_instance"), "task_id") else "UNKNOWN_TASK"
    execution_date = context.get("execution_date", "NOW")
    exception = context.get("exception", "No exception info")
    
    sanitized_error = sanitize_message(str(exception))
    alert_payload = {
        "status": "FAILED",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": str(execution_date),
        "error": sanitized_error
    }
    logger.error(">>> CRITICAL ALERT: Task execution failed in SismoWatch pipeline!")
    logger.error(f"Alert details: {alert_payload}")
    # In production, dispatch to Slack/Discord/PagerDuty webhook here
    return alert_payload

def get_retry_decorator(max_attempts=5, min_wait=1, max_wait=30):
    """Returns a tenacity retry decorator with exponential backoff."""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        reraise=True
    )
