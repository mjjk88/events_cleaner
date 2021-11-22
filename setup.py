from setuptools import setup

setup(
    name='events_cleaner',
    packages=['events_cleaner', 'tests'],
    install_requires=['boto3==1.17.19',
                      'pandas==1.2.3',
                      'pandas-schema==0.3.5'

                      ],
    test_suite='tests',
    tests_require=['unittest-resources', 'testfixtures==6.17.1'],
)
