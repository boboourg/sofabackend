"""Compatibility entrypoint for raw repository tests.

The canonical test module is ``tests.test_storage_raw_repository``; this file
keeps older targeted pytest commands working.
"""

from .test_storage_raw_repository import *  # noqa: F401,F403
