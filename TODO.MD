# Master To-Do list

- [x] **COME UP WITH A GOOD NAME!** (`pandasql` is already taken in PyPI)
- [x] Fix up `setup.py`
- [x] Stream BCP stdout to logs in real time asynchronously
- [x] Upload to PyPI
- [x] Upload to conda-forge
- [x] Set up CI/CD
- [ ] Write more tests for 100% code coverage
- [x] Benchmarks vs. regular pandas methods
- [x] Find way to automate creation of temp SQL Server db for testing
- [ ] Build in auto-retry with native pandas methods if BCP method fails
- [x] Use pandas internal code to build new SQL tables, instead of just all NVARCHAR
- [x] Better docs
- [x] Remove SqlCmd and all references to it
- [ ] Remove the deprecated `read_sql` and all references to it
- [ ] Test inserting dataframe with (all) boolean values
- [x] Implement if dataframe is missing some database columns and/or the columns are out of order
- [ ] Add to docs required SQL permissions to run diff parts, and that bcpandas uses `INFORMATION_SCHEMA` SQL views
- [ ] Add to docs when/if NULL/NaNs are inserted and how

... and of course, anything marked `# TODO` in the code.
