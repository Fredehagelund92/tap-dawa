#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-dawa",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dawa"],
    install_requires=[
        "singer-python==5.4.1",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-dawa=tap_dawa:main
    """,
    packages=["tap_dawa"],
    package_data = {
        "schemas": ["tap_dawa/schemas/*.json"]
    },
    include_package_data=True,
)
