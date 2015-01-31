import requests
import os
import json
import pika
import logging
from string import Template

COMMITS_PER_PAGINATION = 10
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
recv_channel = connection.channel()
recv_channel.queue_declare(queue='fetcher_queue', durable=True)

send_channel = connection.channel()
send_channel.queue_declare(queue='analyser_queue', durable=True)


def get_user_events(user, page=0):
    if not user:
        raise StopIteration
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


def callback(ch, method, properties, body):
    if not body:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.warn("no body in the message targeted to fetcher")
        return
    try:
        body = json.loads(body)
        user = body.get('user')
        logging.info("getting commits for user %s" % user)
        for commit in get_user_events(user):
            message = {'user': user, 'commit': commit}
            send_channel.basic_publish(exchange='',
                                       routing_key='analyser_queue',
                                       body=json.dumps(message),
                                       properties=pika.BasicProperties(
                                           delivery_mode=2,
                                       ))
    except ValueError:
        logging.warn("unparseable body in the message to fetcher")
    ch.basic_ack(delivery_tag=method.delivery_tag)

recv_channel.basic_qos(prefetch_count=1)
recv_channel.basic_consume(callback, queue='fetcher_queue')
logging.info("fetcher starting to consume messages")
recv_channel.start_consuming()
