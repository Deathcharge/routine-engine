from setuptools import setup, find_packages

setup(
    name='routine-engine',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        # Add any dependencies here, e.g., 'sqlalchemy', 'httpx'
    ],
    author='Manus AI',
    description='Helix Collective Routine Engine for multi-agent coordination and execution',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Deathcharge/routine-engine',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    python_requires='>=3.8',
)
