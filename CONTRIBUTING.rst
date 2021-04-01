============
Contributing
============

The research project eGo_n and egon-data are collaborative projects with
several people contributing to it. The following section gives an
overview of applicable guidelines and rules to enable a prospering
collaboration.
Any external contributions are welcome as well, and they are greatly
appreciated! Every little bit helps, and credit will always be given.


Bug reports and feature requests
================================

The best way to report bugs, inform about intended developments, send
feedback or propose a feature
is to file an issue at
https://github.com/openego/eGon-data/issues.

Please tag your issue with one of the predefined labels as it helps
others to keep track of unsolved bugs, open tasks and questions.

To inform others about intended developments please include:
* a describtion of the purpose and the value it adds
* outline the required steps for implementation
* list open questions

When reporting a bug please include all information needed to reproduce
the bug you found.
This may include information on

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.


Contribution guidelines
=======================

Development
-----------

Adding changes to the egon-data repository should follow some guidelines:

1. Create an issue in our `repository
   <https://github.com/openego/eGon-data/issues>`_ to describe the
   intended developments briefly

2. Create a branch for your issue related development from the
   dev-branch following our branch naming convention::

    git checkout -b `<prefix>/#<issue-id>-very-brief-description`

   where `issue-id` is the issue number on GitHub and `prefix` is one of

    - features
    - fixes
    - refactorings

   depending on which one is appropriate. This command creates a new
   branch in your local repository, in which you can now make your
   changes. Be sure to check out our `style conventions`_ so that your
   code is in line with them.

   .. _style conventions: `Code and Commit Style`_

3. Make sure to update the documentation along with your code changes

4. When you're done making changes run all the checks and docs builder
   with `tox <https://tox.readthedocs.io/en/latest/install.html>`_ one
   command::

    tox

5. Commit your changes and push your branch to GitHub::

    git add -p
    git commit
    git push origin features/#<issue-id>-very-brief-description

  Note that the :code:`-p` switch will make :code:`git add` iterate
  through your changes and prompt for each one on whether you want to
  include it in the upcoming commit. This is useful if you made multiple
  changes which should conceptually be grouped into different commits,
  like e.g. fixing the documentation of one function and changing the
  implementation of an unrelated one in parallel, because it allows you
  to still make separate commits for these changes. It has the drawback
  of not picking up new files though, so if you added files and want to
  put them under version control, you have to add them explicitly by
  running :code:`git add FILE1 FILE2 ...` instead.

6. Submit a pull request through the GitHub website.


Code and Commit Style
---------------------

We try the adhere to the `PEP 8 Style Guide <PEP8_>`_ wherever possible.
In addition to that, we use `a code formatter` to have a consistent
style, even in cases where PEP 8 leaves multiple degrees of freedom. So
please run your code through :code:`black` before committing it. [#black]_
PEP 8 also specifies a way to group imports, onto which we put the
additional constraint that the imports within each group are ordered
alphabetically. Once again, you don't have to keep track of this
manually, but you can use `isort`_ to have imports sorted automatically.
Note that `pre-commit` hooks are configured for this repository, so you
can just :code:`pip install pre-commit` followed by :code:`pre-commit
install` in the repository, and every commit will automatically be
checked for style violations.

Unfortunately these tools don't catch everything, so here's a short list
of things you have to keep track of manually:

  - :code:`Black` can't automatically break up overly long strings, so
    make use of Python's automatic string concatenation feature by e.g.
    converting

    .. code-block:: python

      something = "A really really long string"

    into the equivalent:

    .. code-block:: python

      something = (
          "A really really"
          " long string"
      )

  - :code:`Black` also can't check whether you're using readable names
    for your variables. So please don't use abbreviations. Use `readable
    names`_.

  - :code:`Black` also can't reformat your comments. So please keep in
    mind that PEP 8 specifies a line length of 72 for free flowing text
    like comments and docstrings. This also extends to the documentation
    in reStructuredText files.

Last but not least, commit message are a kind of documentation, too,
which should adhere to a certain style. There are quite a few documents
detailing this style, but the shortest and easiest to find is probably
https://commit.style. Try to to commit small, related changes. If you
have to use an "and" when trying to summarize your changes, they should
probably be grouped into separate commits.

.. _a code formatter: https://pypi.org/project/black/
.. _isort: https://pypi.org/project/isort/
.. _pre-commit: https://pre-commit.com
.. _readable names: https://chrisdone.com/posts/german-naming-convention/
.. [#black]
    If you want to be really nice, run any file you touch through
    :code:`black` before making changes, and commit the result
    separately from other changes.. The repository may contain wrongly
    formatted legacy code, and this way you commit eventually necessary
    style fixes separated from your actually meaningful changes, which
    makes the reviewers job a lot easier.

Pull Request Guidelines
-----------------------

We use pull requests (PR) to integrate code changes from branches.
PRs always need to be reviewed (exception proves the rule!). Therefore, ask
one of the other developers for reviewing your changes. Once approved, the PR
can be merged. Please delete the branch after merging.

Before requesting a review, please

1. Include passing tests (run ``tox``). [#tox-note]_
2. Update documentation when there are new functionalities etc.
3. Add a note to ``CHANGELOG.rst`` about the changes and refer to the
   corresponding Github issue.
4. Add yourself to ``AUTHORS.rst``.

.. [#tox-note]
    If you don't have all the necessary Python versions available locally
    you can rely on Travis -
    it will `run the tests`_  for each change you add in the pull request.

    It will be slower though ...

.. _run the tests: https://travis-ci.org/openego/eGon-data/pull_requests


When requesting reviews, please keep in mind it might be a significant effort
to review the PR. Try to make it easier for them and keep the overall effort
as low as possible. Therefore,

* asking for reviewing specific aspects helps reviewers a lot to focus on the
  relevant parts
* when multiple people are asked for a review it should be avoided that they
  check/test the same things. Be even more specific what you expect from
  someone in particular.


What needs to be reviewed?
--------------------------

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


Extending the data workflow
===========================

The egon-data workflow uses Apache Airflow which organizes the order of
different processing steps and their execution.


How to add Python scripts
-------------------------

To integrate a new Python function to the egon-data workflow follow the
steps listed:

1. Add your well documented script to the egon-data repository
2. Integrate functions which need to be called within the workflow to
   pipeline.py, which organzies and calls the different tasks within the
   workflow
3. Define the interdependencies between the scripts by setting the task
   downstream to another required task
4. The workflow can now be triggered via Apache Airflow


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


Documentation
=============

eGon-data could always use more documentation, whether as part of the
official eGon-data docs, in docstrings, or even in articles, blog posts
or similar resources. Always keep in mind to update the documentation
along with your code changes though.


How to document Python scripts
------------------------------

Use docstrings to document your Python code. Note that PEP 8 also
contains a `section <PEP8-docstrings_>`_ on docstrings and that there is
a whole `PEP <PEP257_>`_ dedicated to docstring convetions. Try to
adhere to both of them.
Additionally every Python script needs to contain a header describing
the general functionality and objective and including information on
copyright, license and authors.

.. code-block:: python

   """ Provide an example of the first line of a module docstring.

   This is an example header describing the functionalities of a Python
   script to give the user a general overview of what's happening here.
   """

   __copyright__ = "Example Institut"
   __license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
   __url__ = "https://github.com/openego/eGon-data/blob/main/LICENSE"
   __author__ = "github_alias1, github_alias2"


How to document SQL scripts
---------------------------

Please also add a similar header to your SQL scripts to give users and
fellow developers an insight into your scripts and the methodologies
applied. Please describe the content and objectives of the script
briefly but as detailed as needed to allow other to comprehend how it
works.

.. code-block:: SQL

   /*
   This is an example header describing the functionalities of a SQL
   script to give the user a general overview what's happening here

   __copyright__ = "Example Institut"
   __license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
   __url__ = "https://github.com/openego/eGon-data/blob/main/LICENSE"
   __author__ = "github_alias1, github_alias2"
   */


How-to
======

Tips
----

To run a subset of tests::

    tox -e envname -- pytest -k test_myfeature

To run all the test environments in *parallel*::

    tox -p auto


.. _PEP8: https://www.python.org/dev/peps/pep-0008
.. _PEP8-docstrings: https://www.python.org/dev/peps/pep-0008/#documentation-strings
.. _PEP257: https://www.python.org/dev/peps/pep-0257/
