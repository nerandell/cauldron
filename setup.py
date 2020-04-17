from setuptools import setup

setup(name='cauldron',
      version='1.5.4',
      author='Ankit Chandawala',
      author_email='ankitchandawala@gmail.com',
      url='https://github.com/nerandell/cauldron',
      description='Utils to reduce boilerplate code',
      packages=['cauldron'], install_requires=['aiopg', 'aioredis', 'psycopg2','elasticsearch'])
