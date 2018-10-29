from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name='uit',
    version='0.1.0',
    description="Python wrapper for DoD HPCMP UIT+ rest interface",
    author="Dharhas Pothina",
    author_email='dharhas.pothina@erdc.dren.mil',
    url='https://github.com/dharhas/uit',
    packages=['uit'],
    entry_points={
        'console_scripts': [
            'uit=uit.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='uit',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
