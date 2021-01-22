from os import path
from setuptools import setup, find_packages


here = path.abspath(path.dirname(__file__))

setup_requirements = ['pytest-runner']

test_requirements = ['pytest']

with open(path.join(here, 'changelog.rst')) as f:
    readme = f.read()

setup(
    name='tinybird-beam',
    author='Tinybird',
    author_email='support@tinybird.co',
    description="An Apache Beam Sink Library for Tinybird.",
    install_requires=[
        'requests==2.25.1',
        'apache-beam[gcp]==2.27.0'
    ],
    include_package_data=True,
    packages=find_packages(),
    long_description=readme,
    long_description_content_type='text/x-rst',
    setup_requires=setup_requirements,
    #test_suite='tests',
    #tests_require=test_requirements,
    #url='https://github.com/mitchelllisle/beam-sink',
    version='1.0.0.dev9',
    zip_safe=False,
)
