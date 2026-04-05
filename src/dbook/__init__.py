"""dbook — Database metadata compiler for AI agent consumption.

Built on agentlib (https://github.com/barkain/agentlib).
"""

__version__ = "0.3.0"

from dbook.serializer import book_to_dict, book_to_json, load_book_json, save_book_json

__all__ = ["book_to_dict", "book_to_json", "load_book_json", "save_book_json"]
