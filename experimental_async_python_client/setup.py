from setuptools import setup

import sys
if sys.version_info < (3, 7):
    sys.exit('Python version must be greater than 3.7')

setup(
    name='fast_pubsub',
    version='',
    packages=['fast_client', 'fast_client.core', 'fast_client.reader', 'fast_client.writer', 'fast_client.helpers',
              'fast_client.processor', 'fast_client.subscriber', 'fast_client.ack_processor',
              'fast_client.done_handler'],
    url='',
    license='',
    author='dpcollins',
    author_email='',
    description='',
    install_requires=[
        'tornado>=5',
        'google-auth-oauthlib',
        'ciso8601'
    ]
)
