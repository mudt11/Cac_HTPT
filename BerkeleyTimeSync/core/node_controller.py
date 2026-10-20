"""Compatibility import for the controller moved to controllers/node_controller.py.

New code should import from BerkeleyTimeSync.controllers.node_controller.
This module owns no lifecycle, membership, election or synchronization logic.
"""

from ..controllers.node_controller import (
    ControllerNotice, NodeController, NodeSnapshot, NoticeKind,
)

__all__ = ["ControllerNotice", "NodeController", "NodeSnapshot", "NoticeKind"]
