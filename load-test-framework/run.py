#!/usr/bin/python

import sys
import subprocess


def main(argv):
    arg_list = ['java', '-jar', 'target/flic.jar']
    for arg in argv:
        arg_list.append(arg)
    subprocess.call(arg_list)

if __name__ == '__main__':
    main(sys.argv[1:])
