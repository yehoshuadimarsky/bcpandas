<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Instructions for creating a new release](#instructions-for-creating-a-new-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Instructions for creating a new release
* Change the version in `setup.py`
* Change the version in `bcpandas/__init__.py`
* Update Readme with any new information, as needed
* Commit to git with the message `v{num}`, like `v1.4.0`, push
* Make a new release in Github
    * Add release notes based on the changes
* It will automatically push the new version to PyPI, using GH Actions
* Conda forge will automatically pick up the new version from PyPI within a day or 2, and it automatically create a PR there