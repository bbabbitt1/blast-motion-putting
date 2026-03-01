import pandas as pd
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv
import os
import time
from config import BASE_URL, LOGIN_ENDPOINT, LOGIN_HEADERS, SESSIONS_ENDPOINT, DEFAULT_PARAMS, DATA_ENDPOINT
import json 
from bs4 import BeautifulSoup

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
                putt = json.loads(row[0])
                putt['session_id'] = s['id']
                metrics = {
                    'back_stroke_time':         json.loads(row[1])['value'],
                    'forward_stroke_time':       json.loads(row[2])['value'],
                    'total_stroke_time':         json.loads(row[3])['value'],
                    'tempo':                     json.loads(row[4])['value'],
                    'impact_stroke_speed':       json.loads(row[5])['value'],
                    'back_stroke_length':        json.loads(row[6])['value'],
                    'loft_change':               json.loads(row[7])['value'],
                    'backstroke_rotation':       json.loads(row[8])['value'],
                    'forward_stroke_rotation':   json.loads(row[9])['value'],
                    'face_angle_at_impact':      json.loads(row[10])['value'],
                    'lie_change':                json.loads(row[11])['value'],
                }
                putt.update(metrics)
                all_putts.append(putt)
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Skipping malformed row in session {s['id']}: {e}")
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
    )


def get_with_retry(session, url, params=None, retries=3, wait=10):
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=60)
            return response
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
    raise Exception(f"All {retries} attempts failed for {url}")


def get_high_watermark(engine):
    with open('sql/queries/get_high_watermark.sql') as f:
        query = f.read()
    with engine.connect() as conn:
        result = conn.execute(text(query))
        watermark = result.scalar()
        print(f"High watermark: {watermark}")
        return watermark
