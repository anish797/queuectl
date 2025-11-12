from setuptools import setup, find_packages
setup(
    name='queuectl',
    version='0.1.0',
    description='CLI-based background job queue system',
    author='Your Name',
    py_modules=['cli', 'database', 'config', 'worker', 'launcher'],
    install_requires=[
        'click>=8.0.0',
    ],
    entry_points={
        'console_scripts': [
            'queuectl=cli:cli',
        ],
    },
    python_requires='>=3.7',
)