from setuptools import setup, find_packages

setup(
    name='osdu-commons',
    version='0.7.0',
    packages=find_packages(exclude=['tests', 'tests.*', 'scripts']),
    install_requires=[
        'boto3>=1.9.22,<2.0.0',
        'requests>=2.21.0,<3.0.0',
        'attrs>=18.1.0,<19.0.0',
        'arrow==0.12.1',
        'retrying==1.3.3',
        'cachetools==3.0.0',
        'pampy==0.2.1',
    ]
)
