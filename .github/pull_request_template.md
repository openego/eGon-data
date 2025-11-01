Fixes # .

*Please add a brief description of the changes here.*

### 📋 Pull Request Guidelines

> Please read the [Pull Request Guidelines](https://egon-data.readthedocs.io/en/latest/contributing.html#pull-request-guidelines) carefully before creating your PR.

---

## 🧑‍💻 Contributor Checklist

Before requesting a review, make sure you've completed all of the following:

- [ ] All **tests pass** locally or via CI
      _(for more information on local test, check `tox` in the [Contributing section](https://egon-data.readthedocs.io/en/latest/contributing.html#))_
      _(CI tests are automatically executed when creating a PR, you can see the results of the checks below)_
- [ ] Workflow has run at least once in **Test mode**
      _(optional if no dataset changes are involved)_
- [ ] Relevant **documentation is updated** (API, new features, etc.)
- [ ] Dataset-versions are updated when existing datasets are adjusted.
- [ ] Added a note to `CHANGELOG.rst` about the changes
- [ ] Added yourself to `AUTHORS.rst`

Optional:

- [ ] Changes have been tested in **Everything mode**
- [ ] Extend the checklist for reviewers: Which aspects should be reviewed in particular?

```markdown
<!-- Example:
Please focus on validating the data handling in file XYZ.
-->
```

---

## 🔍 Reviewer Checklist

During your review, please check the following:

- [ ] **Is the code clean, readable, and efficient?** Are there any oddities or obvious inefficiencies?
- [ ] Does the code work as expected? _(should already be verified by contributor)_
- [ ] Do all tests pass? (see CI results)
- [ ] Is the documentation complete and up to date?
- [ ] Is `CHANGELOG.rst` updated accordingly?
- [ ] Is all necessary metadata complete and correct?
  - [ ] If metadata is pending: Is there an appropriate issue filed?


---

## 📝 Additional Notes (optional)

<!-- Add any extra context or known issues here (e.g., performance, design decisions, etc.) -->

---

💡 **Tip:** If you add multiple reviewers, clarify who should check what — this saves time and avoids duplicated efforts.
