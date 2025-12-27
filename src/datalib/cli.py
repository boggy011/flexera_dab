"""
Command-line interface for datalib.
"""

from datalib._version import __version__, __environment__, __branch__, __build_number__


def print_version():
    """Print version information."""
    print(f"datalib version {__version__}")
    print(f"  Environment: {__environment__}")
    print(f"  Branch: {__branch__}")
    print(f"  Build: {__build_number__}")


if __name__ == "__main__":
    print_version()
