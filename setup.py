from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='energyetl',
    version='1.0',
    author='Louis Esan',
    description='Description of my package',
    packages=['package'],
    install_requires=requirements
)
