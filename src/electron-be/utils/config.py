from loguru import logger
def load_timeframe(path="timeframe.txt"):
    """
    Read an integer from a text file indicating minutes per update cycle.
    Defaults to 15 if anything goes wrong.
    """
    try:
        with open(path, "r") as f:
            val = int(f.read().strip())
            if val > 0:
                return val
    except Exception:
        logger.warning(f"Could not read timeframe from {path}, defaulting to 15")
    return 15