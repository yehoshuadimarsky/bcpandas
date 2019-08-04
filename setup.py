from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="bcpandas",
    version="0.1",
    author="Josh Dimarsky",
    description="Microsoft SQL Server bcp (Bulk Copy) wrapper for pandas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yehoshuadimarsky/bcpandas",
    packages=find_packages(exclude=["tests.*", "tests"]), 
    python_requires='>=3.6',
    keywords="bcp mssql pandas",
    classifiers=[
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: SQL",
        "License :: OSI Approved :: MIT License",
        # "Operating System :: OS Independent",
    ],
)
