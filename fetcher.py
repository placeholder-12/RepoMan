import requests
import os
import json
from string import Template

COMMITS_PER_PAGINATION = 10

def get_user_events(user, page=0):
    url = Template('https://api.github.com/users/${user}/events?client_id=${client_id}&client_secret=${client_secret}&per_page=${count}&page=${page}')
    api_params = {
        'user': user,
        'count': COMMITS_PER_PAGINATION,
        'client_id': os.environ.get('CLIENT_ID'),
        'client_secret': os.environ.get('CLIENT_SECRET'),
        'page': page
    }
    while True:
        r = requests.get(url.substitute(api_params))
        d = json.loads(r.text)
        for event in d:
            yield event

        if len(d) < COMMITS_PER_PAGINATION:
            raise StopIteration

        api_params['page'] = api_params['page'] + 1
