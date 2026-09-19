"""
Standalone runner - no FastAPI, no APScheduler.
Runs saas (Mongo SAAS -> Postgres SAAS) then uat (Mongo UAT -> Postgres UAT),
once, then exits. Meant to be triggered by cron.

Usage:
    python3 runner.py
"""
import logging
import saas
import uat

logger = logging.getLogger("runner")

if __name__ == "__main__":
    logger.info("=== Starting uat sync ===")
    try:
        uat.run()
    except Exception as e:
        logger.exception("UAT sync failed")
    logger.info("=== Starting saas sync ===")
    try:
        saas.run()
    except Exception as e:
        logger.exception("SAAS sync failed")

    logger.info("=== Both syncs finished ===")
