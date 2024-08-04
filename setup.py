from setuptools import setup, find_packages

setup(
    name="beam-bigquery-etl-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "apache-beam[gcp]==2.39.0",
        "google-cloud-bigquery==2.34.3",
        "pandas==1.4.2",
        "pyyaml==6.0",
        "python-dotenv==0.19.2",
    ],
)
