#!/usr/bin/env python
"""Runs the load test framework."""

# Copyright 2016 Google Inc. All Rights Reserved.
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

import getopt
import os
import subprocess
import sys


def main(project, test, client_types, vms_count, broker):
  """Runs the load test framework.

  Manages the installation and running of the load test framework
  with basic options.

  Args:
    project: The name of the Google Cloud project.
    test: The type of test to run. Valid options are 'latency', 'throughput',
          and 'service'.
    client_types: The type of clients to run the test against. Valid options
                  are 'gcloud_java', 'gcloud_python', and 'gcloud_ruby' 'vtk'.
    vms_count: The number of VMs to start for each client type. You must have
               sufficient Google Compute Engine quota to start vms_count *
               len(client_types) * 4 cores, and it will take 4 times as much
               quota to run a 'throughput' or 'service' test.
    broker: The network address of the Kafka broker to connect to. If supplied
            we will automatically start Kafka clients, they do not need to be
            specified under client_types.
  """
  if not os.path.isfile('./src/main/resources/gce/driver.jar'):
    subprocess.call(['mvn', 'package'])
    subprocess.call(['cp', 'target/driver.jar', 'target/classes/gce/'])
  if not os.path.isfile('./target/classes/gce/cps.zip'):
    go_package = 'cloud.google.com/go/pubsub/loadtest/cmd'
    go_bin_location = './target/loadtest-go'
    # return_code = subprocess.call(['go', 'build', '-o', go_bin_location, go_package])
    # if return_code != 0:
    #   sys.exit('cannot build Go load tester, maybe run `go get -u {}`?'.format(go_package))

    subprocess.call([
        'zip', './target/classes/gce/cps.zip',
        './python_src/clients/cps_publisher_task.py',
        './python_src/clients/cps_subscriber_task.py',
        './python_src/clients/loadtest_pb2.py',
        './python_src/clients/requirements.txt',
        './ruby_src/Gemfile',
        './ruby_src/loadtest_pb.rb',
        './ruby_src/loadtest_services_pb.rb',
        './ruby_src/cps_publisher_task.rb',
        './ruby_src/cps_subscriber_task.rb'# ,
        # go_bin_location
    ])
  arg_list = ['java', '-jar', 'target/driver.jar', '--project', project]
  gcloud_subscriber_count = 0
  for client in client_types:
    if client == 'gcloud_python':
      arg_list.append('--cps_gcloud_python_publisher_count=' + str(vms_count))
      arg_list.append('--cps_gcloud_python_subscriber_count=' + str(vms_count))
    elif client == 'gcloud_ruby':
      arg_list.append('--cps_gcloud_ruby_publisher_count=' + str(vms_count))
      arg_list.append('--cps_gcloud_ruby_subscriber_count=' + str(vms_count))
    elif client == 'gcloud_java':
      arg_list.append('--cps_gcloud_java_publisher_count=' + str(vms_count))
      arg_list.append('--cps_gcloud_java_subscriber_count=' + str(vms_count))
    elif client == 'gcloud_go':
      arg_list.append('--cps_gcloud_go_publisher_count=' + str(vms_count))
      arg_list.append('--cps_gcloud_go_subscriber_count=' + str(vms_count))
  if broker:
    arg_list.extend([
        '--broker=' + broker, '--kafka_publisher_count=' + str(vms_count),
        '--kafka_subscriber_count=' + str(vms_count)
    ])
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
  elif test == 'service':
    arg_list.extend([
        '--message_size=10000', '--publish_batch_size=10',
        '--request_rate=1000000000', '--max_outstanding_requests=1600',
        '--loadtest_duration=10m', '--burn_in_duration=2m',
        '--publish_batch_duration=50ms', '--cores=16'
    ])
  print(' '.join(arg_list))
  subprocess.call(arg_list)


if __name__ == '__main__':
  project_arg = None
  test_arg = 'latency'
  client_types_arg = set([])
  vms_count_arg = 1
  broker_arg = None
  opts, _ = getopt.getopt(
      sys.argv[1:], '',
      ['client_types=', 'test=', 'vms_count=', 'broker=', 'project='])
  for opt, arg in opts:
    if opt == '--project':
      project_arg = arg
    if opt == '--test':
      test_arg = arg
    if opt == '--client_types':
      client_types_arg = set([])
      for client_type in arg.split(','):
        client_types_arg.add(client_type.strip())
    if opt == '--vms_count':
      vms_count_arg = int(arg)
    if opt == '--broker':
      broker_arg = arg
  if not project_arg:
    sys.exit('You must provide the name of your project with --project.')
  if vms_count_arg < 1:
    sys.exit('If provided, --vms_count must be greater than 0.')
  if test_arg not in ['latency', 'throughput', 'service']:
    sys.exit('Invalid --test parameter given. Must be set to \'latency\', '
             '\'throughput\', or \'service\'. (' + test_arg +
             ') was provided.')
  if len(client_types_arg) == 0:
    client_types_arg = set(['gcloud_java'])
  if not client_types_arg.issubset(
      set(['gcloud_python', 'gcloud_java', 'gcloud_go', 'gcloud_ruby'])):
    sys.exit(
        'Invalid --client_type parameter given. Must be a comma deliminated '
        'sequence of client types. Allowed client types are \'gcloud_python\', '
        '\'gcloud_java\', and \'gcloud_ruby\'. (' + ','.join(
            client_types_arg) +
        ') was provided. Kafka is assumed if --broker is provided.')
  main(project_arg, test_arg, client_types_arg, vms_count_arg, broker_arg)
