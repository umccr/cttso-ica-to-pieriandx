import pkg_resources
import setuptools


with open('requirements.txt') as fh:
    requirements = [str(r) for r in pkg_resources.parse_requirements(fh)]

setuptools.setup(
    name='lambda_utils',
    version='0.0.1',
    description='metapackage for cttso-ica-to-pieriandx Lambda functions',
    author='UMCCR and contributors',
    license='GPLv3',
    packages=setuptools.find_packages(),
    install_requires=requirements,
)
