from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='harvaestus',
    version='0.0.1',
    packages=find_packages(),
    url='',
    license='',
    author='Jakob Rößler',
    author_email='',
    description='',
    install_requires=requirements,
)
