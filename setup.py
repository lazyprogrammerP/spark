from setuptools import find_packages, setup

with open("requirements.txt") as fd:
    install_requires = fd.read().splitlines()

setup(
    name="spark",
    version="0.0.1",
    description="Python SDK for Newron Model Logging",
    url="https://www.newron.ai",
    author="Parshva Runwal",
    author_email="parshva@newron.ai",
    license="MIT",
    keywords="",
    packages=find_packages(),
    install_requires=install_requires,
)
