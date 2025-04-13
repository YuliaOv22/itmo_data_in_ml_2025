from setuptools import find_packages, setup

setup(
    name="real_fake_news_project",
    packages=find_packages(exclude=["real_fake_news_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
