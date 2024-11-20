# Master To-Do List

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Completed](#completed)
- [Open](#open)
  - [General](#general)
  - [Docs](#docs)
  - [CI/CD](#cicd)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Completed
- [x] **COME UP WITH A GOOD NAME!** (`pandasql` is already taken in PyPI)
- [x] Fix up `setup.py`
- [x] Stream BCP stdout to logs in real time asynchronously
- [x] Upload to PyPI
- [x] Upload to conda-forge
- [x] Set up CI/CD
- [x] Benchmarks vs. regular pandas methods
- [x] Find way to automate creation of temp SQL Server db for testing
- [x] Use pandas internal code to build new SQL tables, instead of just all NVARCHAR
- [x] Better docs
- [x] Remove SqlCmd and all references to it
- [x] Remove the deprecated `read_sql` and all references to it
- [x] Test inserting dataframe with (all) boolean values
- [x] Implement if dataframe is missing some database columns and/or the columns are out of order
- [x] Add section in docs detailing testing and Docker
- [x] `1.0.0` Release

## Open

### General
- [ ] Explore alternatives to `INFORMATION_SCHEMA` SQL views
- [ ] Write more tests for 100% code coverage
- [ ] Build in auto-retry with native pandas methods if BCP method fails

### Docs
- [ ] Add instructions for contributing (setup conda env, how to write tests, pre-commit, etc.)
- [ ] Add to docs the exact required SQL permissions

### CI/CD
- [ ] Add the benchmarks to CI/CD
- [ ] Find way to use Linux containers on Windows OS in CI/CD


... and of course, anything marked `# TODO` in the code.
