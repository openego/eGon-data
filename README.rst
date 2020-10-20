========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |coveralls| |codecov|
        | |scrutinizer| |codacy| |codeclimate|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/eGon-data/badge/?style=flat
    :target: https://egon-data.readthedocs.io
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/openego/eGon-data.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/openego/eGon-data

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/openego/eGon-data?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/openego/eGon-data

.. |requires| image:: https://requires.io/github/openego/eGon-data/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/openego/eGon-data/requirements/?branch=master

.. |coveralls| image:: https://coveralls.io/repos/openego/eGon-data/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/openego/eGon-data

.. |codecov| image:: https://codecov.io/gh/openego/eGon-data/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/openego/eGon-data

.. |codacy| image:: https://img.shields.io/codacy/grade/d639ac4296a04edb8da5c882ea36e98b.svg
    :target: https://www.codacy.com/app/openego/eGon-data
    :alt: Codacy Code Quality Status

.. |codeclimate| image:: https://codeclimate.com/github/openego/eGon-data/badges/gpa.svg
   :target: https://codeclimate.com/github/openego/eGon-data
   :alt: CodeClimate Quality Status

.. |version| image:: https://img.shields.io/pypi/v/egon.data.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/egon.data

.. |wheel| image:: https://img.shields.io/pypi/wheel/egon.data.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/egon.data

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/egon.data.svg
    :alt: Supported versions
    :target: https://pypi.org/project/egon.data

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/egon.data.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/egon.data

.. |commits-since| image:: https://img.shields.io/github/commits-since/openego/eGon-data/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/openego/eGon-data/compare/v0.0.0...master


.. |scrutinizer| image:: https://img.shields.io/scrutinizer/quality/g/openego/eGon-data/master.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/openego/eGon-data/


.. end-badges

The data used in the eGo^N project along with the code importing, generating and processing it.

* Free software: GNU Affero General Public License v3 or later (AGPLv3+)

Installation
============

::

    pip install egon.data

You can also install the in-development version with::

    pip install https://github.com/openego/eGon-data/archive/master.zip


Documentation
=============


https://eGon-data.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
