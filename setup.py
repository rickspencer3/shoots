from setuptools import setup, find_packages

def read_requirements(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines() if line.strip()]
    
setup(
    name='shoots',
    version='0.1.1',
    author='Rick Spencer',
    author_email='richard.linger.spencer.3@gmail.com',
    description='A Pandas dataframe data lake',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/rickspencer3/shoots',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires=read_requirements('requirements.txt'),
    extras_require={
        'test': read_requirements('requirements-test.txt'),
    },

    entry_points={
        'console_scripts': [
            'shoots-server=shoots.shoots_server:main',
        ],
    },
)
