def _normalize_path(*parts: str) -> str:
    """Join path parts with single slashes, stripping extras."""
    cleaned = [str(p).strip("/") for p in parts if p]
    return "/".join(cleaned) if cleaned else ""
