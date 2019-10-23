# Master To-Do list

- [x] **COME UP WITH A GOOD NAME!** (`pandasql` is already taken in PyPI)
- [x] Fix up `setup.py`
- [ ] Stream BCP stdout to logs in real time asynchronously
- [x] Upload to PyPI
- [x] Upload to conda-forge
- [ ] Set up CI/CD
- [ ] Write more tests
- [ ] Benchmarks vs. regular pandas methods
- [x] Find way to automate creation of temp SQL Server db for testing
- [ ] Build in auto-retry with native pandas methods if BCP method fails
- [ ] Use pandas internal code to build new SQL tables, instead of just all NVARCHAR
- [ ] Decide if to assume the user provides their own `conn` connection object for native pandas methods,
        Or just use the bcpandas SqlCreds


... and of course, anything marked `# TODO` in the code.
