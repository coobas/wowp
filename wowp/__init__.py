'''WOWP -- A WOrkfloW Framework in Python
'''

__version__ = '0.0.2'
__release__ = __version__ + '-alpha'

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from .components import Component, Actor, Workflow
from . import actors

__all__ = "actors", "Component", "Actor", "Workflow"
