import setuptools

# https://stackoverflow.com/a/33685899/8717022 why requirements.txt and requirements are different
requirements = [
    "pandas",
    "pandas_gbq",
    "pytz",
    "google-auth",
    "google-cloud-bigquery",
    "google-cloud-logging",
    "google-cloud-secret-manager",
    "bigquery_schema_generator"
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pygbq",
    version="0.22",
    author="Zigfrid Zvezdin",
    author_email="ziggerzz@gmail.com",
    description="Easily integrate data in BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ZiggerZZ/pygbq",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
