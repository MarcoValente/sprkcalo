from setuptools import setup, find_packages

setup(
    name="SparkAnalysis",
    version="0.1.0",
    description="ATLAS FastCaloSim Spark-based analysis tools",
    author="Marco Valente",
    author_email="marco.valente@cern.ch",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "pyspark",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    scripts=["sprkcalo/cli.py"],
    entry_points={
        "console_scripts": [
            "sprkcalo=sprkcalo.cli:main",
        ],
    },
    package_data={
        "sprkcalo": ["config/*.yaml", "data/*.csv"],
    },
    include_package_data=True,
)