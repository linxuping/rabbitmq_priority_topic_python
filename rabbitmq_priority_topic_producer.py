#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Usage
python simple_test_send_priority.py 100000
'''
import pika
import sys

credentials = pika.PlainCredentials('test_user', 'test_user')
connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='192.168.29.131', credentials=credentials))
channel = connection.channel() 
channel.exchange_declare(exchange='topic_logs',
                         type='topic')
#channel.queue_declare(queue='linxpq', durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
channel.queue_declare(queue='linxpq1', durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
channel.queue_declare(queue='linxpq2', durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority":10})
channel.queue_bind(exchange='topic_logs', queue='linxpq1', routing_key="zb.*")
channel.queue_bind(exchange='topic_logs', queue='linxpq2', routing_key="zb")

times = 1
if len(sys.argv) > 1:
  times = int(sys.argv[1])
sendstr = "If you're having trouble going through this tutorial you can contact us through the mailing list."
count = 0
import time
for i in range(times):
	for j in range(10):
		rkey = "zb.yy"
		if j%2 == 0:
			rkey = "zb"
		channel.basic_publish(exchange='topic_logs',
				routing_key=rkey,
				body=u'%s. %s.%s'%(sendstr, i,j),
				properties=pika.BasicProperties(
					delivery_mode = 2,
					priority = j, # make message persistent
					))
		#print "%d.%d"%(i,j)
		count += 1
	'''
	channel.basic_publish(exchange='topic_logs',
	      routing_key='zb.yy4',
	      body=u'你好!world. 6',
	      properties=pika.BasicProperties(
		 delivery_mode = 2,
		 priority = 6, # make message persistent
	      ))
channel.basic_publish(exchange='topic_logs',
	      routing_key='zb',
	      body=u'你好!world. 8',
	      properties=pika.BasicProperties(
		 delivery_mode = 2,
		 priority = 8, # make message persistent
	      ))
#print " [x] Sent %r:%r" % (routing_key, message)
#print "fini."
'''
connection.close()
