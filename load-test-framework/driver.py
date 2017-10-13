#!/usr/bin/env python

# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import getopt
import subprocess

def main(project, test, vms_count, zone, sheet_id,
    broker, zookeeper, partitions, replication, mapped, cps):

  """
  Runs the load test framework.

  Args:
    project: The name of the Google Cloud project.

    test: The type of test to run. Valid options are 'latency', 'service',
          'throughput', 'test_throughput', 'ordering', 'correctness'.

    vms_count: The number of VMs to start for each client type. You must have
               sufficient Google Compute Engine quota to start vms_count *
               len(client_types) * 4 cores, and it will take 4 times as much
               quota to run a 'throughput' or 'service' test.

    zone: the region to instantiate the instances in.

    sheet_id: the Google Sheets ID, needed to publish results to.

    broker: The network address of the Kafka broker to connect to. If supplied
            we will automatically start Kafka clients.

    zookeeper: The address of the zookeeper cluster, we can create and manage topics
               automatically if provided.

    partitions: The number of partitions in a single topic - Kafka config.

    replication: The replication factor for every topic - Kafka config.

    mapped: A flag to run the test for the mapped API or not.

    cps: A flag to run the test for CPS or not.

  Arguments Example:

  --project=<PROJECT_NAME> --vms_count=<# of VMs> --test=correctness --mapped --cps
  --sheet_id=<SHEET_ID> --broker=<BROKER_IP:BROKER_PORT>
  --zookeeper=<ZOOKEEPER1_IP:ZOOKEEPER1_PORT,ZOOKEEPER2_IP:ZOOKEEPER2_PORT,..>
  """

  subprocess.call(['mvn', 'clean', 'install'], cwd='../pubsub-mapped-api')
  subprocess.call(['mvn', 'clean', 'package'])

  if not os.path.isfile('./src/main/resources/gce/driver.jar'):
    subprocess.call(['cp', 'target/driver.jar', 'target/classes/gce/'])

  arg_list = ['java', '-Xmx32G', '-jar', 'target/driver.jar', '--project', project]

  # CPS
  if cps:
    arg_list.append('--cps_gcloud_java_publisher_count=' + str(vms_count))
    arg_list.append('--cps_gcloud_java_subscriber_count=' + str(vms_count))

  # Kafka
  if len(broker) != 0:
    arg_list.append('--broker=' + broker)
    arg_list.append('--kafka_publisher_count=' + str(vms_count))
    arg_list.append('--kafka_subscriber_count=' + str(vms_count))
    arg_list.append('--partitions=' + str(partitions))
    arg_list.append('--replication_factor=' + str(replication))

  if len(zookeeper) != 0:
    arg_list.append('--zookeeper_ip_address=' + zookeeper)

  # Mapped
  if mapped:
    arg_list.append('--kafka_mapped_java_publisher_count=' + str(vms_count))
    arg_list.append('--kafka_mapped_java_subscriber_count=' + str(vms_count))

  if test == 'latency':
    arg_list.extend([
        '--message_size=1', '--publish_batch_size=1', '--request_rate=1',
        '--max_outstanding_requests=10', '--loadtest_duration=10m',
        '--burn_in_duration=2m', '--publish_batch_duration=1ms'
    ])
  elif test == 'throughput':
    arg_list.extend([
        '--message_size=10000', '--publish_batch_size=10',
        '--request_rate=1000000000', '--max_outstanding_requests=1600',
        '--loadtest_duration=10m', '--burn_in_duration=2m',
        '--publish_batch_duration=50ms', '--num_cores_test'
    ])
  elif test == 'test_throughput':
    arg_list.extend([
      '--message_size=10000', '--publish_batch_size=10',
      '--request_rate=1000000000', '--max_outstanding_requests=200',
      '--loadtest_duration=2m', '--burn_in_duration=1m',
      '--publish_batch_duration=50ms', '--cores=2'
    ])
  elif test == 'service':
    arg_list.extend([
        '--message_size=10000', '--publish_batch_size=10',
        '--request_rate=1000000000', '--max_outstanding_requests=1600',
        '--loadtest_duration=10m', '--burn_in_duration=2m',
        '--publish_batch_duration=50ms', '--cores=16',
    ])
  elif test == 'ordering':
    arg_list.extend([
        '--order_test', '--message_size=1',
        '--number_of_messages=100000', '--publish_batch_size=100',
        '--request_rate=1000', '--max_outstanding_requests=100',
        '--burn_in_duration=1m', '--publish_batch_duration=1m'
    ])
  elif test == 'correctness':
    arg_list.extend([
      '--message_size=1', '--publish_batch_duration=30s',
      '--number_of_messages=1000000', '--publish_batch_size=1',
      '--request_rate=10000', '--max_outstanding_requests=1000',
      '--burn_in_duration=1m'
    ])

  if len(sheet_id) != 0:
    arg_list.append('--spreadsheet_id=' + sheet_id)

  arg_list.append('--zone=' + zone)

  print(' '.join(arg_list))
  subprocess.call(arg_list)


if __name__ == '__main__':

  broker = ''
  sheet_id = ''
  zookeeper = ''
  partitions = 0
  replication = 0
  vms_count = 1
  project = None
  test = 'latency'
  zone = 'us-central1-a'

  cps = False
  mapped = False

  opts, _ = getopt.getopt(
      sys.argv[1:], '',
      ['vms_count=', 'test=', 'project=', 'zone=',
       'sheet_id=', 'broker=', 'zookeeper=', 'partitions=', 'replication=',
       'mapped', 'cps'])

  for opt, arg in opts:
    if opt == '--test':
      test = arg
    if opt == '--project':
      project = arg
    if opt == '--vms_count':
      vms_count = int(arg)
    if opt == '--partitions':
      partitions = int(arg)
    if opt == '--replication':
      replication = int(arg)
    if opt == '--zone':
      zone = arg
    if opt == '--sheet_id':
      sheet_id = arg
    if opt == '--cps':
      cps = True
    if opt == '--broker':
      broker = arg
    if opt == '--zookeeper':
      zookeeper = arg
    if opt == '--mapped':
      mapped = True

  if not cps and not mapped and len(broker) == 0:
    cps = True
  if not project:
    sys.exit('You must provide the name of your project with --project.')
  if vms_count < 1:
    sys.exit('If provided, --vms_count must be greater than 0.')
  if len(broker) != 0 and partitions < 1:
    sys.exit('If provided, --partitions must be greater than 0.')
  if len(broker) != 0 and replication < 1:
    sys.exit('If provided, --replication must be greater than 0.')
  if test not in ['latency', 'service',
                  'throughput', 'test_throughput',
                  'ordering', 'correctness']:
    sys.exit('Invalid --test parameter given.')

  main(project, test, vms_count, zone, sheet_id,
       broker, zookeeper, partitions, replication, mapped, cps)