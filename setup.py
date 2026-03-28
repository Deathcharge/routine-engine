"""
Routine Engine - Workflow Automation Platform
Setup configuration for pip installation
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="routine-engine",
    version="1.0.0",
    author="Andrew John Ward",
    author_email="contact@helix-collective.dev",
    description="A powerful workflow automation platform for AI-driven applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Deathcharge/routine-engine",
    project_urls={
        "Bug Tracker": "https://github.com/Deathcharge/routine-engine/issues",
        "Documentation": "https://routine-engine.readthedocs.io",
        "Source Code": "https://github.com/Deathcharge/routine-engine",
    },
    packages=find_packages(exclude=["tests", "examples", "docs"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
        "Topic :: Internet :: WWW/HTTP",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "fastapi>=0.100.0",
        "uvicorn>=0.23.0",
        "sqlalchemy>=2.0.0",
        "pydantic>=2.0.0",
        "httpx>=0.24.0",
        "redis>=5.0.0",
        "aioredis>=2.0.0",
        "python-dotenv>=1.0.0",
        "cryptography>=41.0.0",
        "python-multipart>=0.0.6",
        "aiofiles>=23.0.0",
        "psycopg2-binary>=2.9.0",
        "APScheduler>=3.10.0",
        "discord.py>=2.3.0",
        "boto3>=1.28.0",
        "airtable-python-wrapper>=0.15.0",
        "google-api-python-client>=2.100.0",
        "slack-sdk>=3.23.0",
        "PyGithub>=2.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.9.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
            "isort>=5.12.0",
        ],
        "docs": [
            "sphinx>=7.2.0",
            "sphinx-rtd-theme>=1.3.0",
            "sphinx-autodoc-typehints>=1.24.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "routine-engine=routine_engine.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
