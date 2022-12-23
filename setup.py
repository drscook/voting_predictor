from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

# This gets deployed when a new release is made by github actions
VERSION = '{{VERSION_PLACEHOLDER}}'
REQUIRES_PYTHON = '>=3.8'

# CHANGEME VARS
PACKAGE_NAME = 'voting_predictor'
DESCRIPTION = 'voting_predictor'
LONG_DESCRIPTION = 'voting_predictor'
AUTHOR_NAME = 'Scott Cook'
AUTHOR_EMAIL = 'scook@tarleton.edu'
PROJECT_URL = 'https://github.com/voting_predictor'
# REQUIRED_PACKAGES = ['helpers_drscook', 'geopandas']
REQUIRED_PACKAGES = ['git+https://github.com/drscook/helpers', 'geopandas']

PROJECT_KEYWORDS = ['pypi', 'python']
# Read more about classifiers at
# https://pypi.org/classifiers/
CLASSIFIERS = [
        "Programming Language :: Python :: 3"
]

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author=AUTHOR_NAME,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    url = PROJECT_URL,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    keywords=PROJECT_KEYWORDS,
    classifiers=CLASSIFIERS
)
