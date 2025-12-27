"""
Version management for datalib.

This module is updated by the build scripts based on environment:
- dev: {major}.{minor}.{patch}.dev{build}+{branch}
- qa:  {major}.{minor+1}.0rc{build}
- prd: Same as qa (promoted)
"""

__version__ = "1.0.0.dev0+local"
__version_info__ = (1, 0, 0, "dev0", "local")

# Environment metadata
__environment__ = "local"
__build_number__ = "0"
__branch__ = "local"
