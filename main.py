import requests
from dotenv import load_dotenv
import pandas as pd
from utils import clean_putts_df, clean_sessions_df, get_engine, login, get_sessions, get_putts, get_csrf_token, get_high_watermark
from config import BASE_URL, LOGIN_ENDPOINT, SESSIONS_ENDPOINT, DATA_ENDPOINT, DEFAULT_PARAMS, TIMEOUT

load_dotenv()

if __name__ == '__main__':
    session = requests.Session()

    # Step 1: GET CSRF Token
    token = get_csrf_token(session)

    # Step 2: POST login
    login_response = login(session, token)

    # Step 3: Get all sessions
    sessions_list, sessions_data = get_sessions(session)

    # Step 4: Get Session High Watermark 
    engine = get_engine()
    max_session_id = get_high_watermark(engine)
    new_sessions = [s for s in sessions_list if s['id'] > max_session_id]
    print(f"New sessions found: {len(new_sessions)}")

    # Step 4: Fetch putts for each session
    all_putts = get_putts(session, new_sessions)

    # Step 5: Clean dataframes
    df_putts = clean_putts_df(pd.DataFrame(all_putts))
    sessions_df = clean_sessions_df(pd.DataFrame(new_sessions))

    # Step 6: Load to PostgreSQL
    sessions_df.to_sql('blast_sessions', engine, schema='src', if_exists='append', index=False)
    print(f"Loaded {len(sessions_df)} sessions")

    df_putts.to_sql('blast_putts', engine, schema='src', if_exists='append', index=False)
    print(f"Loaded {len(df_putts)} putts")