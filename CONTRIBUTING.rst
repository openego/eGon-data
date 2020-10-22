============
Contributing
============

The research project eGo_n and egon-data are collaborative projects with several people contributing to it. The following section gives an overview of applicable guidelines and rules to enable a prospering collaboration. 
Any external contributions are welcome as well, and they are greatly appreciated! Every little bit helps, and credit will always be given.

Contribution guidelines
=======================

Development
-----------

Adding changes to the egon-data repository should follow some guidelines:  


1. Create an issue in our `repository <https://github.com/openego/eGon-data/issues>`_

2. Create a branch for your issue related development from `dev` following our branch naming convention::

    git checkout -b `features/#<issued-id>-very-brief-description`
   
   Further prefixes are `fixes/` or `refactorings/`.

   Now you can make your changes locally.

3. Make sure to update the documentation along with your code changes

4. When you're done making changes run all the checks and docs builder with `tox <https://tox.readthedocs.io/en/latest/install.html>`_ one command::

    tox

5. Commit your changes and push your branch to GitHub::

    git add .
    git commit -m "Your detailed description of your changes."
    git push origin features/#<issued-id>-very-brief-description

6. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

All changes or additions in the code need to be reviewed by a fellow developer before merging your changes to the `dev`. To trigger this review you should submit a pull request through the Github website. 

For merging, you should:

1. Include passing tests (run ``tox``) [1]_.
2. Update documentation when there's new API, functionality etc.
3. Add a note to ``CHANGELOG.rst`` about the changes.
4. Add yourself to ``AUTHORS.rst``.

.. [1] If you don't have all the necessary python versions available locally you can rely on Travis - it will
       `run the tests <https://travis-ci.org/openego/eGon-data/pull_requests>`_ for each change you add in the pull request.

       It will be slower though ...

Documentation
-------------

eGon-data could always use more documentation, whether as part of the official eGon-data docs, in docstrings, or even on the web in blog posts, articles, and such.


Tips
----

To run a subset of tests::

    tox -e envname -- pytest -k test_myfeature

To run all the test environments in *parallel*::

    tox -p auto




Bug reports and feature requests
================================

The best way to report bugs, send feedback or propose a feature is to file an issue at https://github.com/openego/eGon-data/issues.

When `reporting a bug <https://github.com/openego/eGon-data/issues>`_ please include all information needed to reproduce the bug you found. 
This may include information on

    * Your operating system name and version.
    * Any details about your local setup that might be helpful in troubleshooting.
    * Detailed steps to reproduce the bug. 

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.

