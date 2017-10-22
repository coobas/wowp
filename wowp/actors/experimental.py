from .special import GeneratorActor, LineReader, IteratorActor, Splitter, Chain
import warnings


warnings.simplefilter('always', DeprecationWarning)  # turn off filter
warnings.warn("This module is deprecated, use actors.special",
              category=DeprecationWarning,
              stacklevel=2)
warnings.simplefilter('default', DeprecationWarning)  # reset filter
