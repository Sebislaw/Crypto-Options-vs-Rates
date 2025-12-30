import datetime
import pytz


def current_quarter_timestamp_et() -> int:
    """
    Return the Unix timestamp (UTC seconds) for the start of the current
    15-minute block, based on Eastern Time (ET).

    Returns:
        int: Unix timestamp for the start of the current 15-minute block.
    """
    eastern = pytz.timezone("America/New_York")
    now_et = datetime.datetime.now(eastern)

    # Determine start of current 15-min block
    minute_block = (now_et.minute // 15) * 15
    quarter_start_et = now_et.replace(minute=minute_block, second=0, microsecond=0)

    # Convert that ET time to UTC for timestamp consistency
    quarter_start_utc = quarter_start_et.astimezone(pytz.UTC)
    return int(quarter_start_utc.timestamp())
