#!/usr/bin/env python
import pika
import json
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='fetcher_queue', durable=True)

message = {'user': sys.argv[1]}
channel.basic_publish(exchange='',
                      routing_key='fetcher_queue',
                      body=json.dumps(message),
                      properties=pika.BasicProperties(
                          delivery_mode=2,  # make message persistent
                      ))
print " [x] Sent %r" % (message,)
connection.close()
