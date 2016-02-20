'''WOWP -- A WOrkfloW Framework in Python
'''
from __future__ import absolute_import, division, print_function, unicode_literals
from .__about__ import __version__, __release__

from .components import Component, Actor, Workflow
from . import actors

__all__ = "actors", "Component", "Actor", "Workflow"
