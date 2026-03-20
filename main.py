import requests
import logging
from dotenv import load_dotenv
import pandas as pd

from exceptions import BlastAPIError, BlastAuthError
from logger import get_logger, get_run_id
from utils import (
    clean_putts_df,
    clean_sessions_df,
    get_engine,
    login,
    get_sessions,
    get_putts,
    get_csrf_token,
    get_high_watermark,
    upsert_sessions,
    upsert_putts
)
from config import BASE_URL, LOGIN_ENDPOINT, SESSIONS_ENDPOINT, DATA_ENDPOINT, DEFAULT_PARAMS, TIMEOUT

load_dotenv()

# Initialize logger and run ID
run_id = get_run_id()
logger = get_logger('blast_pipeline')

old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.run_id = run_id
    return record
logging.setLogRecordFactory(record_factory)


def run_pipeline():
    logger.info("Starting Blast Motion pipeline")

    try:
        session = requests.Session()

        # Step 1: GET CSRF Token
        logger.info("Fetching CSRF token")
        token = get_csrf_token(session)

        # Step 2: POST login
        logger.info("Logging in to Blast Connect")
        login_response = login(session, token)

        # Step 3: Get all sessions
        logger.info("Fetching session list")
        sessions_list, sessions_data = get_sessions(session)

        # Step 4: Get high watermark
        engine = get_engine()
        max_session_id = int(get_high_watermark(engine))
        logger.info(f"High watermark: {max_session_id}")

        # Step 5: Filter to new sessions only
        new_sessions = [s for s in sessions_list if int(s['id']) > max_session_id]
        logger.info(f"New sessions found: {len(new_sessions)}")

        if not new_sessions:
            logger.info("No new sessions found - exiting pipeline")
        else:
            # Step 6: Fetch putts for new sessions only
            logger.info("Fetching putts for new sessions")
            all_putts = get_putts(session, new_sessions)

            # Step 7: Clean dataframes
            logger.info("Cleaning dataframes")
            df_putts = clean_putts_df(pd.DataFrame(all_putts))
            sessions_df = clean_sessions_df(pd.DataFrame(new_sessions))

            # Step 8: Load to Azure SQL
            logger.info("Loading to Azure SQL")
            with engine.begin() as conn:
                try:
                    upsert_sessions(sessions_df, engine)
                    upsert_putts(df_putts, engine)
                    logger.info("Transaction committed successfully")
                except Exception as e:
                    logger.error(f"Transaction failed, rolling back: {e}")
                    raise

    except BlastAuthError as e:
        logger.error(f"Authentication failed: {e}")
        raise
    except BlastAPIError as e:
        logger.error(f"API error: {e}")
        raise
    except Exception as e:
        logger.critical(f"Unexpected pipeline failure: {e}")
        raise


if __name__ == '__main__':
    run_pipeline()