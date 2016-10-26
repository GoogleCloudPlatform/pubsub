#!/usr/bin/python

import subprocess
import sys


def main(argv):
    arg_list = ['java', '-jar', 'target/driver.jar']
    for arg in argv:
        arg_list.append(arg)
    subprocess.call(arg_list)

if __name__ == '__main__':
    main(sys.argv[1:])
