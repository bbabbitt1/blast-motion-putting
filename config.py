BASE_URL = 'https://golf-academy.blastconnect.com'
LOGIN_ENDPOINT = '/login'
SESSIONS_ENDPOINT = '/blast/filter-action-type'
DATA_ENDPOINT = '/blast/data-table'
TIMEOUT = 45
METRICS = 'back_stroke_time|forward_stroke_time|total_stroke_time|tempo|impact_stroke_speed|back_stroke_length|loft|back_stroke_rotation|forward_stroke_rotation|rotation_change|lie'

MAX_RETRIES = 3
BASE_WAIT = 2
DB_SCHEMA = 'src'

LOGIN_HEADERS = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': f'{BASE_URL}{LOGIN_ENDPOINT}'
}

DEFAULT_PARAMS = {
    'action_type': 'Putt',
    'swing_type': 'all_putts',
    'video_only': '0',
    'metric_order': METRICS,
    'start': '0',
    'length': '500'
}

METRIC_INDEX_MAP = {
    'back_stroke_time':         1,
    'forward_stroke_time':      2,
    'total_stroke_time':        3,
    'tempo':                    4,
    'impact_stroke_speed':      5,
    'back_stroke_length':       6,
    'loft_change':              7,
    'backstroke_rotation':      8,
    'forward_stroke_rotation':  9,
    'face_angle_at_impact':     10,
    'lie_change':               11,
}

