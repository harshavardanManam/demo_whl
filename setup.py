from setuptools import setup, find_packages

setup(
  name = 'protobuf_generators',
  version = '1.3.1',
  packages = find_packages(),
  entry_points={
    'console_scripts': [
      'envelope_generator = protobuf_generators.envelope_generator:main'
    ],
  }
)