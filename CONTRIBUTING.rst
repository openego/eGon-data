============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.


Extending the data workflow
===========================

Where to save (downloaded) data?
--------------------------------

If a task requires to retrieve some data from external sources which needs to
be saved locally, please use `CWD` to store the data. This is achieved by using

.. code-block:: python

  from pathlib import Path
  from urllib.request import urlretrieve

  filepath = Path(".") / "filename.csv"
  urlretrieve("https://url/to/file", filepath)


Adjusting test mode data
------------------------

When integrating new data or data processing scripts, make sure the
:ref:`Test mode` still works correctly on a limited subset of data.
In particular, if a new external data sources gets integrated make sure the
data gets cut to the region of the test mode.


Bug reports
===========

When `reporting a bug <https://github.com/openego/eGon-data/issues>`_ please include:

    * Your operating system name and version.
    * Any details about your local setup that might be helpful in troubleshooting.
    * Detailed steps to reproduce the bug.

Documentation improvements
==========================

eGo^N Data could always use more documentation, whether as part of the
official eGo^N Data docs, in docstrings, or even on the web in blog posts,
articles, and such.

Feature requests and feedback
=============================

The best way to send feedback is to file an issue at https://github.com/openego/eGon-data/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that code contributions are welcome :)

Development
===========

To set up `eGon-data` for local development (points (1) and (2) are only
relevant for people who are not members of the organization
`openego <https://github.com/openego>`_):

1. Fork `eGon-data <https://github.com/openego/eGon-data>`_
   (look for the "Fork" button).
2. Clone your fork locally::

    git clone git@github.com:YOURGITHUBNAME/eGon-data.git

3. Create a branch for local development::

    git checkout -b name-of-your-bugfix-or-feature

   Consider to use the following pattern for naming branches
   `<features/fixes>/#<issue-number>-<minimal-description-as-name>`.
   Now you can make your changes locally.

4. When you're done making changes run all the checks and docs builder with
`tox <https://tox.readthedocs.io/en/latest/install.html>`_ one command:

   .. code-block:: bash

      tox

5. Commit your changes and push your branch to GitHub::

    git add .
    git commit -m "Your detailed description of your changes."
    git push origin name-of-your-bugfix-or-feature

6. Submit a pull request through the GitHub website.

Tips
^^^^

To run a subset of tests::

    tox -e envname -- pytest -k test_myfeature

To run all the test environments in *parallel*::

    tox -p auto


Pull Request Guidelines
-----------------------

We use pull requests (PR) to integrate code changes from branches.
PRs always need to be reviewed (exception proves the rule!). Therefore, ask
one of the other developers for reviewing your changes. Once approved, the PR
can be merged. Please delete the branch after merging.

Before requesting a review, please

1. Include passing tests (run ``tox``) [1]_.
2. Let the workflow run in :ref:`Test mode` once from scratch to verify
   successful execution
3. Update documentation when there's new API, functionality etc.
4. Add a note to ``CHANGELOG.rst`` about the changes.
5. Add yourself to ``AUTHORS.rst``.

When requesting reviews, please keep in mind it might be a significant effort
to review the PR. Try to make it easier for them and keep the overall effort
as low as possible. Therefore,

* asking for reviewing specific aspects helps reviewers a lot to focus on the
  relevant parts
* when multiple people are asked for a review it should be avoided that they
  check/test the same things. Be even more specific what you expect from
  someone in particular.


What needs to be reviewed?
^^^^^^^^^^^^^^^^^^^^^^^^^^

Things that definitely should be checked during a review of a PR:

* *Is the code working?* The contributor should already have made sure that
  this is the case. Either by automated test or manual execution.
* *Is the data correct?* Verifying that newly integrated and processed data
  is correct is usually not possible during reviewing a PR. If it is necessary,
  please ask the reviewer specifically for this.
* *Do tests pass?* See automatic checks.
* *Is the documentation up-to-date?* Please check this.
* *Was* ``CHANGELOG.rst`` *updated accordingly?* Should be the case, please
  verify.
* *Is metadata complete and correct (in case of data integration)?* Please
  verify. In case of a pending metadata creation make sure an appropriate issue is filed.


Additional case-dependent aspects make sense.



.. [1] If you don't have all the necessary python versions available locally you can rely on
       `CI on GitHub actions <https://github.com/openego/eGon-data/actions?query=workflow%3A%22Tests%2C+code+style+%26+coverage%22>`_
       - it will run the tests for each change you add in the pull request.

       It will be slower though ...


Extending the data workflow
---------------------------


Adjusting test mode data
^^^^^^^^^^^^^^^^^^^^^^^^

When integrating new data or data processing scripts, make sure the
:ref:`Test mode` still works correctly on a limited subset of data.
In particular, if a new external data sources gets integrated make sure the
data gets cut to the region of the test mode.


Bug reports and feature requests
================================

When `reporting a bug <https://github.com/openego/eGon-data/issues>`_ please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that code contributions are welcome :)

Documentation improvements
==========================

eGo^N Data could always use more documentation, whether as part of the
official eGo^N Data docs, in docstrings, or even on the web in blog posts,
articles, and such.
Please make sure you change the documentation along with code changes.

The changes of the documentation in a feature branch get visible once a pull
request is opened.

You can build the documentation locally with (executed in the repos root
directory)

.. code-block:: bash

   sphinx-build -E -a docs docs/_build/

Eventually, you might need to install additional dependencies for building the
documenmtation:

.. code-block:: bash

   pip install -r docs/requirements.txt

