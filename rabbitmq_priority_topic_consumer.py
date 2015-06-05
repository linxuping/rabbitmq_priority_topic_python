#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Usage
python receive_logs_topic.py linxpq1
python receive_logs_topic.py linxpq2
'''
import pika
import sys

queuename = sys.argv[1]

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='192.168.29.131'))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs',
                         type='topic')
'''
channel.queue_declare(queue='linxpq1', durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
channel.queue_declare(queue='linxpq2', durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
channel.queue_bind(exchange='topic_logs', queue='linxp1', routing_key="zb.*")
channel.queue_bind(exchange='topic_logs', queue='linxp2', routing_key="zb")
'''
#result = channel.queue_declare(queue=queuename, durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
#queue_name = result.method.queue
queue_name = queuename

print ' [*] Waiting for logs. To exit press CTRL+C'

'''
def callback(ch, method, properties, body):
    print ch, method, properties, body
    print " [x] %r:%r" % (method.routing_key, body,)
    #channel.basic_ack(delivery_tag = method.delivery_tag)
channel.basic_qos(prefetch_count=3)
channel.basic_consume(callback, #这种做法不能回调取到priority queue items，只能尝试其他方法
                      queue=queue_name,
                      no_ack=False)
channel.start_consuming()
'''
import time
count = 0
while 1: #blocked reactor
  r = channel.basic_get(queue=queue_name, no_ack=False) #0
  if r[0] != None:
    channel.basic_nack(delivery_tag=r[0].delivery_tag, multiple=False, requeue=False)
    count += 1
    if count%100 == 0:
        print r[-1], r[0].delivery_tag
    print count
  #time.sleep(0.2)
connection.close()

