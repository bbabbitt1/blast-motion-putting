import pandas as pd
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv
import os
import time
import random
from exceptions import *
from logger import get_logger
from config import BASE_URL, LOGIN_ENDPOINT, LOGIN_HEADERS, SESSIONS_ENDPOINT, DEFAULT_PARAMS, DATA_ENDPOINT, TIMEOUT, METRIC_INDEX_MAP, MAX_RETRIES, BASE_WAIT, DB_SCHEMA
import json 
from bs4 import BeautifulSoup
import requests
from sqlalchemy.dialects.postgresql import insert


logger = get_logger('blast_utils')

def login(session, token):
    payload = {
        'email': os.getenv('BLAST_EMAIL'),
        'password': os.getenv('BLAST_PASSWORD'),
        '_token': token
    }
    response = session.post(
        f'{BASE_URL}{LOGIN_ENDPOINT}',
        data=payload,
        headers=LOGIN_HEADERS
    )
    
    if 'blast' not in response.url:
        raise Exception(f"Login failed - redirected to {response.url}")
    
    print(f"Login successful - redirected to {response.url}")
    return response

def get_csrf_token(session):
    login_page = get_with_retry(session, f'{BASE_URL}{LOGIN_ENDPOINT}')
    soup = BeautifulSoup(login_page.text, 'html.parser')
    
    try:
        token = soup.find('input', {'name': '_token'})['value']
        print("Token found")
        return token
    except TypeError:
        raise Exception("Token not found - check if Blast page structure has changed")
    
def get_sessions(session):
    response = get_with_retry(
        session,
        f'{BASE_URL}{SESSIONS_ENDPOINT}',
        params={'action_type': 'Putt'}
    )
    
    try:
        data = json.loads(response.text)
        sessions_list = [s for s in data['sessions'] if s['id'] != '']
        print(f"Found {len(sessions_list)} sessions")
        return sessions_list, data
    except (KeyError, json.JSONDecodeError) as e:
        raise Exception(f"Failed to parse sessions response: {e}")

def get_putts(session, sessions_list):
    all_putts = []
    
    for s in sessions_list:
        print(f"Fetching putts for session: {s['name']}")
        
        params = {**DEFAULT_PARAMS, 'sessions[]': s['id']}
        
        response = get_with_retry(
            session,
            f'{BASE_URL}{DATA_ENDPOINT}',
            params=params
        )
        
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            print(f"Failed to parse response for session {s['id']}: {e}")
            continue
        
        for row in data['data']:
                try:
                    putt = parse_putt_row(row)
                    putt['session_id'] = s['id']
                    all_putts.append(putt)
                except BlastParseError as e:
                    logger.warning(f"Skipping malformed row in session {s['id']}: {e}")
                    continue
        
        print(f"  → {len(data['data'])} putts fetched")
    
    print(f"Total putts fetched: {len(all_putts)}")
    return all_putts

SESSION_TABLE_MAPPING = {
    'id': 'blast_session_id',
    'session_type_id': 'session_type_id',
    'name': 'session_name'
}

def clean_sessions_df(df: pd.DataFrame) -> pd.DataFrame:
    
    # Rename columns first
    df = df.rename(columns=SESSION_TABLE_MAPPING)
    
    # Drop the header row BEFORE casting to int
    df = df[df['blast_session_id'] != '']
    
    # Now safe to cast to int
    df['blast_session_id'] = df['blast_session_id'].astype(int)
    
    return df


def clean_putts_df(df: pd.DataFrame) -> pd.DataFrame:
    
    # Rename date column
    df = df.rename(columns={'date': 'putt_date'})
    
    # Parse date string to timestamp
    df['putt_date'] = pd.to_datetime(df['putt_date'], format='%B %d, %Y / %I:%M%p')
    
    # Drop columns not needed in table
    df = df.drop(columns=['video', 'sport', 'action_type', 'gender', 'thumb'])
    
    # Cast is_air_swing to boolean
    df['is_air_swing'] = df['is_air_swing'].astype(bool)
    
    # Cast metric columns to float
    metric_cols = [
        'back_stroke_time', 'forward_stroke_time', 'total_stroke_time',
        'tempo', 'impact_stroke_speed', 'back_stroke_length', 'loft_change',
        'backstroke_rotation', 'forward_stroke_rotation',
        'face_angle_at_impact', 'lie_change'
    ]
    df[metric_cols] = df[metric_cols].astype(float)
    
    return df

load_dotenv()

def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        connect_args={"sslmode": "disable"}
    )


def get_with_retry(session, url, params=None, retries=MAX_RETRIES, base_wait=BASE_WAIT):

    last_exception = None

    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=TIMEOUT)

            # Check for bad HTTP status codes explicitly
            if response.status_code == 200:
                return response
            elif response.status_code == 401:
                raise BlastAuthError(f"Unauthorized - session may have expired: {url}")
            elif response.status_code == 429:
                raise BlastRateLimitError(f"Rate limited by server: {url}")
            elif response.status_code >= 500:
                raise BlastServerError(f"Server error {response.status_code}: {url}")
            else:
                raise BlastAPIError(f"Unexpected status {response.status_code}: {url}")

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                BlastServerError) as e:
            last_exception = e
            if attempt < retries - 1:
                # Exponential backoff + jitter
                wait = (base_wait ** attempt) + random.uniform(0, 1)
                print(f"Attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {wait:.2f} seconds...")
                time.sleep(wait)
            continue

        except (BlastAuthError, BlastRateLimitError) as e:
            # Don't retry auth or rate limit errors - fail immediately
            raise

    raise BlastAPIError(f"All {retries} attempts failed for {url}: {last_exception}")


def get_high_watermark(engine):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    query_path = os.path.join(base_dir, 'sql', 'queries', 'get_high_watermark.sql')
    
    with open(query_path) as f:
        query = f.read()
    with engine.connect() as conn:
        result = conn.execute(text(query))
        watermark = result.scalar()
        logger.info(f"High watermark: {watermark}")
        return watermark


def parse_putt_row(row: list) -> dict:
    """
    Parse a single putt row from the API response.
    Validates that all expected metrics are present.
    """
    try:
        putt = json.loads(row[0])
    except (json.JSONDecodeError, IndexError) as e:
        raise BlastParseError(f"Failed to parse putt metadata: {e}")

    metrics = {}
    for metric_name, index in METRIC_INDEX_MAP.items():
        try:
            parsed = json.loads(row[index])
            if 'value' not in parsed:
                raise BlastParseError(f"Missing 'value' key for metric '{metric_name}' at index {index}")
            metrics[metric_name] = parsed['value']
        except IndexError:
            raise BlastParseError(f"Row too short - missing metric '{metric_name}' at index {index}")
        except json.JSONDecodeError as e:
            raise BlastParseError(f"Failed to parse metric '{metric_name}' at index {index}: {e}")

    putt.update(metrics)
    return putt

def upsert_sessions(df: pd.DataFrame, engine) -> None:
    """Insert new sessions, skip if already exists"""
    records = df.to_dict(orient='records')
    
    with engine.begin() as conn:  # engine.begin() auto commits or rolls back
        stmt = insert(get_sessions_table(engine)).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['blast_session_id'])
        conn.execute(stmt)
        logger.info(f"Upserted {len(records)} sessions")


def upsert_putts(df: pd.DataFrame, engine) -> None:
    """Insert new putts, skip if already exists"""
    records = df.to_dict(orient='records')
    
    with engine.begin() as conn:
        stmt = insert(get_putts_table(engine)).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['blast_id'])
        conn.execute(stmt)
        logger.info(f"Upserted {len(records)} putts")


def get_sessions_table(engine):
    from sqlalchemy import MetaData, Table
    metadata = MetaData()
    return Table('blast_sessions', metadata, autoload_with=engine, schema=DB_SCHEMA)


def get_putts_table(engine):
    from sqlalchemy import MetaData, Table
    metadata = MetaData()
    return Table('blast_putts', metadata, autoload_with=engine, schema=DB_SCHEMA)