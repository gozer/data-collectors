from setuptools import setup

setup(
    name='data-collectors',
    version='0.1.0',
    author='Aaron Wirick',
    author_email='awirick@mozilla.com',
    description='Python tool for collecting data and loading it into vertica',
    url='https://github.com/mozilla-it/data-collectors',
    packages=['collectors'],
    data_files = [('collectors/defaults',['collectors/defaults/default_log_config.yml'])],
    python_requires='>=3.4',
    license='MPL-2.0',
    install_requires=[
        'Click',
        'pyyaml',
        'pandas',
        'pyodbc'
    ],
    tests_require=['pytest', 'pytest-cov', 'pytest-flake8'],
    setup_requires=['pytest-runner'],
    entry_points='''
        [console_scripts]
        data-collectors=collectors:main
    ''',
)
