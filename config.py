BASE_URL = 'https://golf-academy.blastconnect.com'
LOGIN_ENDPOINT = '/login'
SESSIONS_ENDPOINT = '/blast/filter-action-type'
DATA_ENDPOINT = '/blast/data-table'
TIMEOUT = 45
METRICS = 'back_stroke_time|forward_stroke_time|total_stroke_time|tempo|impact_stroke_speed|back_stroke_length|loft|back_stroke_rotation|forward_stroke_rotation|rotation_change|lie'

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

