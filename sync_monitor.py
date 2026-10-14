"""
Run bookkeeping for common_sync (no sync logic here):
  - sync window: fetch from the last successful run, so a failed run's
    period is re-covered by the next run
  - error collection for one run
  - failure alert mail (SMTP)
"""
import os
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Normal runs fetch from the last successful run (minus the overlap), so the
# window stays ~15 min + run time. The caller's window_minutes is only the
# fallback when there is no saved state yet (first run / new server).
SYNC_OVERLAP_MINUTES    = 10
SYNC_MAX_WINDOW_MINUTES = int(os.getenv("SYNC_MAX_WINDOW_MINUTES", 1440))


def _state_file(name: str) -> str:
    """Last successful run time per pipeline. Kept next to the code (not /tmp)
    so it survives a server reboot."""
    state_dir = os.getenv("SYNC_STATE_DIR") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ".sync_state"
    )
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, f"last_success_{name}.txt")


def load_last_success(name: str):
    try:
        with open(_state_file(name), "r") as f:
            return datetime.fromisoformat(f.read().strip())
    except (OSError, ValueError):
        return None


def save_last_success(name: str, ts: datetime):
    path = _state_file(name)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(ts.isoformat())
    os.replace(tmp_path, path)


def get_sync_time_filter(name: str, run_started: datetime, window_minutes: int) -> datetime:
    """A failed run never saves its time, so the next run automatically
    re-covers that period."""
    last_success = load_last_success(name)
    if not last_success:
        return run_started - timedelta(minutes=window_minutes)

    time_filter = last_success - timedelta(minutes=SYNC_OVERLAP_MINUTES)
    oldest_allowed = run_started - timedelta(minutes=SYNC_MAX_WINDOW_MINUTES)
    if time_filter < oldest_allowed:
        logger.warning(
            f"Last success was {last_success.isoformat()}; capping window to "
            f"{SYNC_MAX_WINDOW_MINUTES} minutes. Run with INITIAL_RUN to backfill older changes."
        )
        return oldest_allowed
    return time_filter


class ErrorCollector(logging.Handler):
    """Collects every ERROR logged during one run, so table-level failures
    (which are logged and swallowed) still mark the run as failed."""
    def __init__(self, target_logger: logging.Logger):
        super().__init__(level=logging.ERROR)
        self.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self.records = []
        self.target_logger = target_logger
        target_logger.addHandler(self)

    def emit(self, record):
        self.records.append(self.format(record))

    def detach(self):
        self.target_logger.removeHandler(self)


def send_failure_alert(name: str, errors: list):
    """SMTP alert mail. Configure via .env:
    SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_FROM,
    ALERT_EMAIL_TO (comma separated)."""
    smtp_host = os.getenv("SMTP_HOST")
    to_addrs = [a.strip() for a in (os.getenv("ALERT_EMAIL_TO") or "").split(",") if a.strip()]
    if not (smtp_host and to_addrs):
        logger.warning("Alert email not configured (SMTP_HOST / ALERT_EMAIL_TO missing); skipping.")
        return

    smtp_user = os.getenv("SMTP_USER")
    shown = errors[:50]
    body = "\n\n".join(shown)
    if len(errors) > len(shown):
        body += f"\n\n... and {len(errors) - len(shown)} more errors (see server logs)."

    msg = EmailMessage()
    msg["Subject"] = f"[mdb-pdb-sync] {name.upper()} sync FAILED ({len(errors)} errors)"
    msg["From"] = os.getenv("ALERT_EMAIL_FROM") or smtp_user
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(f"{name.upper()} sync had errors at {datetime.now(timezone.utc).isoformat()}:\n\n{body}")

    try:
        with smtplib.SMTP(smtp_host, int(os.getenv("SMTP_PORT", 587)), timeout=30) as smtp:
            smtp.starttls()
            if smtp_user:
                smtp.login(smtp_user, os.getenv("SMTP_PASSWORD"))
            smtp.send_message(msg)
        logger.info(f"Failure alert sent to {', '.join(to_addrs)}.")
    except Exception as e:
        logger.warning(f"Could not send alert email: {e}")


def alert_if_lock_stuck(name: str, lock_file: str, stale_seconds: int, error: Exception):
    """Another run holds the lock. That is normal, unless it has been held
    longer than stale_seconds (hung process)."""
    try:
        lock_age = datetime.now().timestamp() - os.path.getmtime(lock_file)
    except OSError:
        return
    if lock_age > stale_seconds:
        send_failure_alert(name, [
            f"Sync looks stuck: lock held for {int(lock_age // 60)} minutes. {error}"
        ])


def finish_run(name: str, run_started: datetime, error_collector: ErrorCollector):
    """Errors -> alert mail and keep the old last_success (next run retries
    this period). Clean run -> move last_success forward."""
    error_collector.detach()
    if error_collector.records:
        send_failure_alert(name, error_collector.records)
        return
    try:
        save_last_success(name, run_started)
    except OSError as e:
        logger.warning(f"Could not save last success time: {e}")
