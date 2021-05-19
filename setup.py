import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tinyvoc",
    version="0.0.1",
    author="Frank Dekervel",
    author_email="frank@kapernikov.com",
    description="tiny VOC utilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="??",
    packages=setuptools.find_packages(),
    install_requires=[
        "PyYAML>=5.4.1",
        "lxml>=4.6.3"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.7',
    entry_points = {
        'console_scripts':[
            'merge-annotations=tinyvoc.merge_annotations:main',
            'prepare-annotations=tinyvoc.prepare_annotations:main',
        ]
    }
)
