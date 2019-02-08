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
