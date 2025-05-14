#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import, print_function

from glob import glob
from os.path import basename, dirname, join, splitext
import io
import re

from setuptools import find_packages, setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ) as fh:
        return fh.read()


setup(
    name="egon.data",
    version="1.0.0",
    license="AGPL-3.0-or-later",
    description=(
        "The data used in the eGo^N project along with the code importing, "
        "generating and processing it."
    ),
    long_description="%s\n%s"
    % (
        re.compile("^.. start-badges.*^.. end-badges", re.M | re.S).sub(
            "", read("README.rst")
        ),
        re.sub(":[a-z]+:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst")),
    ),
    author=read("AUTHORS.rst"),
    author_email=(
        "jonathan.amme@rl-institut.de, "
        "clara.buettner@hs-flensburg.de, "
        "carlos.epia@hs-flensburg.de, "
        "kilian.helfenbein@rl-institut.de"
    ),
    url="https://github.com/openego/eGon-data",
    packages=["egon"] + ["egon." + p for p in find_packages("src/egon")],
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list:
        # http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        (
            "License :: OSI Approved :: "
            "GNU Affero General Public License v3 or later (AGPLv3+)"
        ),
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        # uncomment if you test on these interpreters:
        # "Programming Language :: Python :: Implementation :: PyPy",
        # "Programming Language :: Python :: Implementation :: IronPython",
        # "Programming Language :: Python :: Implementation :: Jython",
        # "Programming Language :: Python :: Implementation :: Stackless",
        "Topic :: Utilities",
    ],
    project_urls={
        "Documentation": "https://eGon-data.readthedocs.io/",
        "Changelog": (
            "https://eGon-data.readthedocs.io/en/latest/changelog.html"
        ),
        "Issue Tracker": "https://github.com/openego/eGon-data/issues",
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires=">=3.8",
    install_requires=[
        # eg: 'aspectlib==1.1.1', 'six>=1.7',
        "apache-airflow>=1.10.14,<2.0",  # See accompanying commit message
        "atlite==0.2.5",
        "cdsapi",
        "click",
        "geopandas>=0.10.0,<0.11.0",
        "geopy",
        "geovoronoi==0.3.0",
        "importlib-resources",
        "loguru",
        "markupsafe<2.1.0",  # MarkupSafe>=2.1.0 breaks WTForms<3
        "matplotlib",
        "netcdf4",
        "numpy<1.23",  # incompatibilities with shapely 1.7.
        # See: https://stackoverflow.com/a/73354885/12460232
        "oedialect==0.0.8",
        "omi",
        "openpyxl",
        "pandas>1.2.0,<1.4",  # pandas>=1.4 needs SQLAlchemy>=1.4
        "psycopg2",
        "pyaml",
        "pypsa==0.17.1",
        "rasterio",
        "rioxarray",
        "rtree",
        "saio",
        "seaborn",
        "shapely",
        "snakemake<7",
        "sqlalchemy<1.4",  # Airflow<2.0 is not compatible with SQLAlchemy>=1.4
        "wtforms<3",  # WTForms>=3.0 breaks Airflow<2.0
        "xarray",
        "xlrd",
    ],
    extras_require={
        "dev": ["black", "flake8", "isort>=5", "pre-commit", "pytest", "tox"]
        # eg:
        #   'rst': ['docutils>=0.11'],
        #   ':python_version=="2.6"': ['argparse'],
    },
    entry_points={"console_scripts": ["egon-data = egon.data.cli:main"]},
)
