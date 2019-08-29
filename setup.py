import setuptools

# import ``__version__`` from code base
exec(open('prosto/version.py').read())

setuptools.setup(
    name='prosto',
    version=__version__,

    # descriptive metadata for upload to PyPI
    description='Prosto data processing toolkit',
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    author='Prosto Data',
    author_email='52108119+prostodata@users.noreply.github.com',
    license='MIT License',
    keywords = ['data processing', 'feature engineering', 'data science', 'analytics', 'machine learning', 'data mining', 'forecasting', 'time series', 'pandas'],
    url='https://github.com/prostodata',
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
        'numpy>=1.16',
        'pandas>=0.24',
    ],
    zip_safe=True,

    # package content (what to include)
    packages=setuptools.find_packages(),

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.md', '*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        #'hello': ['*.msg'],
    },

    # It will generate prosto.exe and prosto-script.py in the Scripts folder of Python
    #entry_points={
    #    'console_scripts': [
    #        'prosto=prosto.main:main'
    #    ],
    #},
    # The files will be copied to Scripts folder of Python
    scripts=[
        #'scripts/prosto.bat',
        #'scripts/prosto',
        #'scripts/prosto.py',
    ],
)
