from setuptools import setup

install_requires = [
    'flask', 'PyYAML', 'requests', 'dodcerts',
]

extras_require = dict()

extras_require['guitools'] = ['panel', 'param', 'holoviews', 'pandas']

extras_require['examples'] = ['jupyterlab', 'nodejs'] + extras_require['guitools']

extras_require['tests'] = ['pytest', 'flake8']

setup(
    name='pyuit',
    version='0.6.1',
    description="Python wrapper for DoD HPCMP UIT+ REST interface",
    author="Scott Christensen",
    author_email='scott.d.christensen@erdc.dren.mil',
    url='https://github.com/erdc/uit',
    packages=['uit', 'uit.gui_tools'],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'uit=uit.cli:cli'
        ]
    },
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.7",
    keywords='uit',
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
