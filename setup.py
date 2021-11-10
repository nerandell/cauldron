from setuptools import setup

setup(name='cauldron',
      version='1.5.5',
      author='Ankit Chandawala',
      author_email='ankitchandawala@gmail.com',
      url='https://github.com/nerandell/cauldron',
      description='Utils to reduce boilerplate code',
      packages=['cauldron'],
      install_requires=['aioredis==0.3.3', 'psycopg2==2.8.6', 'aiopg==0.12.0', 'elasticsearch', 'aiohttp==0.17.4'])
