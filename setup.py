from setuptools import find_packages, setup

setup(
    name="quality_data_pipelines",
    packages=find_packages(exclude=["quality_data_pipelines_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
