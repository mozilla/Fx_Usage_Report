#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    'coverage',
    'pytest',
    'pytest-cov',
    'pytest-timeout',
    'moto',
    'mock',
]

extras = {
    'testing': test_deps,
}

setup(
    name='fx_usage_report',
    version='0.1',
    description='Python ETL job for the Firefox Usage Report',
    author='Firefox Public Data Platform',
    author_email='fx-public-data@mozilla.com',
    url='https://github.com/mozilla/Fx_Usage_Report.git',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[
        'arrow==0.10.0',
        'click==6.7',
        'click_datetime==0.2',
        'numpy==1.13.3',
        'pyspark==2.2.0.post0',
        'scipy==1.0.0rc1',
    ],
    tests_require=test_deps,
    extras_require=extras,
)
