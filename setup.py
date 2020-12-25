import setuptools

# import ``__version__`` from code base
exec(open('prosto/__init__.py').read())

setuptools.setup(
    name='prosto',
    version=__version__,

    # descriptive metadata for upload to PyPI
    description='Data processing toolkit radically changing the way data is processed',
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    author='Alexandr Savinov',
    author_email='savinov@conceptoriented.org',
    license='MIT License',
    keywords = ['data processing', 'analytics', 'data science', 'pandas', 'map-reduce', 'feature engineering', 'business intelligence'],
    url='https://github.com/asavinov/prosto',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers ",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
    ],

    test_suite='nose.collector',
    tests_require=['nose'],

    # dependencies
    install_requires=[
        'numpy',
        'pandas',
    ],
    zip_safe=True,

    # package content (what to include)
    packages=setuptools.find_packages(),
    #packages=setuptools.find_packages(exclude=("tests",)),
    #packages=["prosto", "notebooks", "tests"],

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.md', '*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        #'hello': ['*.msg'],
    },

)
