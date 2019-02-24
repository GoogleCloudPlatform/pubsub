# Copyright 2019 Google LLC
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

import subprocess
import sys

if __name__ == '__main__':
    java_args = ['java', '-jar', 'target/driver.jar'] + sys.argv[1:]

    subprocess.call(['mvn', 'package'])
    subprocess.call(['cp', 'target/driver.jar', 'target/classes/gce/'])

    subprocess.call([
        'zip', '-FSr', './target/classes/gce/cps.zip', './proto',
        './python_src', './node_src/src', './node_src/package.json', './go_src'
    ])

    subprocess.call(java_args)
