"""Token counting utilities for dbook."""


def count_tokens(text: str) -> int:
    """Count tokens in text. Uses tiktoken if available, else estimates ~4 chars/token."""
    try:
        import tiktoken  # type: ignore[reportMissingImports]
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except ImportError:
        return len(text) // 4
