
def canon_email(e: str) -> str:
    e = (e or "").strip().lower()
    # simple cleanups
    if e.startswith("<") and e.endswith(">"):
        e = e[1:-1]
    return e
