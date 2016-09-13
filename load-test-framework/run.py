#!/usr/bin/python

import sys
import subprocess
import time

BASE_EXEC_PATH = 'java -Xmx20g -jar target/flic.jar '


def kafka_pub_load_test():
  # Runs a test which publishes to Kafka
  args = BASE_EXEC_PATH + \
  '--num_messages 10000000 \
  --message_size 10 \
  --topics [YOUR TOPIC HERE] \
  kafka \
  --broker [YOUR BROKER HOST+PORT HERE]'

  subprocess.call(args, shell=True)


def cps_pub_load_test():
  # Runs a test which publishes to Cloud Pub/Sub
  args = BASE_EXEC_PATH + \
  '--num_messages 10000000 \
  --message_size 10 \
  --topics [YOUR TOPIC HERE] \
  cps \
  --batch_size 1000 \
  --response_threads 5 \
  --num_clients 5 \
  --rate_limit 1000 \
  --project [YOUR CPS PROJECT HERE]'

  subprocess.call(args, shell=True)


def cps_pubsub_load_test():
  # Runs a test which publishes and consumes from Cloud Pub/Sub
  args = BASE_EXEC_PATH + \
  '--num_messages 100000 \
  --publish false \
  --message_size 10 \
  --topics [YOUR TOPIC HERE] \
  cps \
  --response_threads 5 \
  --num_clients 5 \
  --rate_limit 1000 \
  --project [YOUR CPS PROJECT HERE]'

  result1 = subprocess.Popen(args, shell=True)
  time.sleep(10)
  args = BASE_EXEC_PATH + \
  '--num_messages 100000 \
  --message_size 10 \
  --topics [YOUR TOPIC HERE] \
  cps \
  --batch_size 1000 \
  --response_threads 5 \
  --num_clients 5 \
  --rate_limit 1000 \
  --project [YOUR CPS PROJECT HERE]'

  result2 = subprocess.Popen(args, shell=True)
  result2.communicate()
  result1.communicate()


def custom(argv):
  arg_list = ['java', '-jar', 'target/flic.jar']
  for arg in argv:
    arg_list.append(arg)
  subprocess.call(arg_list)

if __name__ == '__main__':
  custom(sys.argv[1:])
