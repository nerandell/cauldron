from setuptools import setup

setup(name='cauldron',
      version='1.0.14',
      author='Ankit Chandawala',
      author_email='ankitchandawala@gmail.com',
      url='https://github.com/nerandell/cauldron',
      description='Utils to reduce boilerplate code',
      packages=['cauldron'], install_requires=['aiopg', 'aioredis', 'psycopg2'])
