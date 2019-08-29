from setuptools import setup

install_requires = [
    'flask', 'PyYAML', 'requests',
]

extras_require = dict()

extras_require['recommended'] = ['dodcerts']

extras_require['examples'] = (['jupyterlab', 'nodejs', 'panel', 'param', 'holoviews', 'pandas'])

extras_require['tests'] = (['pytest', 'flake8'])

setup(
    name='pyuit',
    version='0.3.0a9',
    description="Python wrapper for DoD HPCMP UIT+ rest interface",
    author="Dharhas Pothina",
    author_email='dharhas.pothina@erdc.dren.mil',
    url='https://github.com/erdc/uit',
    packages=['uit'],
    entry_points={
        'console_scripts': [
            'uit=uit.cli:cli'
        ]
    },
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.6",
    keywords='uit',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
