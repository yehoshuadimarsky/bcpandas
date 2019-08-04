import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pandasql",
    version="0.1",
    author="Josh Dimarsky",
    description="Microsoft SQL Server bcp (Bulk Copy) wrapper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yehoshuadimarsky/pandasql",
    packages=setuptools.find_packages(),
    keywords="bcp mssql",
    classifiers=[
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: SQL",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
