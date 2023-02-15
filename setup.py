from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='energyetl',
    version='1.0',
    author='Louis Esan',
    email="louisgedo@hotmail.com",
    description="A package to implement a data pipeline for downloading and cleaning energy trend data which is available on the UK government's website",
    packages=['energyetl'],
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'energyetl=energyetl.main:main'
          ]
    }
)