"""
SCID to NinjaTrader 8 Converter Setup
"""

from setuptools import setup, find_packages
import os

# Read README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="scid-converter",
    version="1.0.0",
    author="SCID Converter Team",
    author_email="your-email@example.com",
    description="High-performance SCID to NinjaTrader 8 converter with automatic cleaning",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/scid-converter",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/scid-converter/issues",
        "Documentation": "https://github.com/yourusername/scid-converter/wiki",
        "Source Code": "https://github.com/yourusername/scid-converter",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Environment :: Console",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=21.0.0",
            "flake8>=3.8.0",
        ],
        "performance": [
            "numpy>=1.19.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "scid-converter=scid_converter:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="scid ninjatrader converter tick-data financial trading",
)
