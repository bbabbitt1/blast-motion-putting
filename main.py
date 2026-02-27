import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import pandas as pd
import re
import json

load_dotenv()
session = requests.Session()


# Step 1: GET login page - captures session cookie AND token together
login_page = session.get('https://golf-academy.blastconnect.com/login')
soup = BeautifulSoup(login_page.text, 'html.parser')
token = soup.find('input', {'name': '_token'})['value']
print(f"Token: {token}")
print(f"Cookies after GET: {session.cookies.get_dict()}")

# Step 2: POST login with matching token + session
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': 'https://golf-academy.blastconnect.com/login'
}
payload = {
    'email': os.getenv('BLAST_EMAIL'),
    'password': os.getenv('BLAST_PASSWORD'),
    '_token': token
}
login_response = session.post(
    'https://golf-academy.blastconnect.com/login',
    data=payload,
    headers=headers
)
print(f"Login status: {login_response.status_code}")
print(f"Cookies after POST: {session.cookies.get_dict()}")
print(f"Redirected to: {login_response.url}")
# Step 3: GET putting data
data_response = session.get(
    'https://golf-academy.blastconnect.com/blast/data-table',
    params={
        'action_type': 'Putt',
        'swing_type': 'all_swings',
        'video_only': '0',
        'metric_order': 'back_stroke_time|forward_stroke_time|total_stroke_time|tempo|impact_stroke_speed|back_stroke_length|loft|back_stroke_rotation|forward_stroke_rotation|rotation_change|lie',
        'start': '0',
        'length': '500'
    }
)
sessions_response = session.get(
    'https://golf-academy.blastconnect.com/blast/filter-action-type',
    params={'action_type': 'Putt'},
    timeout=30
)
print("Session Info:")
print(sessions_response.text[:2000])
print()
print()
data = json.loads(data_response.text)

# Each row in data['data'] is a list, first element is a JSON string
rows = []
for row in data['data']:
    swing = json.loads(row[0])  # metadata
    metrics = {
        'back_stroke_time': json.loads(row[1])['value'],
        'forward_stroke_time': json.loads(row[2])['value'],
        'total_stroke_time': json.loads(row[3])['value'],
        'tempo': json.loads(row[4])['value'],
        'impact_stroke_speed': json.loads(row[5])['value'],
        'back_stroke_length': json.loads(row[6])['value'],
        'loft_change': json.loads(row[7])['value'],
        'backstroke_rotation': json.loads(row[8])['value'],
        'forward_stroke_rotation': json.loads(row[9])['value'],
        'face_angle_at_impact': json.loads(row[10])['value'],
        'lie_change': json.loads(row[11])['value'],
    }
    swing.update(metrics)
    rows.append(swing)

df = pd.DataFrame(rows)



df.to_csv("putting_data.csv")
