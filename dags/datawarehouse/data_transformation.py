from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

def parse_duration(duration_str: str) -> timedelta:
    """
    Zamienia ISO 8601 duration (np. 'PT37S', 'PT23M31S') na timedelta
    """
    if not duration_str.startswith("P"):
        raise ValueError("Invalid ISO 8601 duration")

    duration_str = duration_str.replace("P", "").replace("T", "")

    days, hours, minutes, seconds = 0, 0, 0, 0

    # szukamy godzin, minut i sekund
    if "H" in duration_str:
        hours_str, duration_str = duration_str.split("H")
        hours = int(hours_str)
    if "M" in duration_str:
        minutes_str, duration_str = duration_str.split("M")
        minutes = int(minutes_str)
    if "S" in duration_str:
        seconds_str, duration_str = duration_str.split("S")
        seconds = int(seconds_str)

    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def transform_data(row):
    """
    Zamienia row['Duration'] z ISO 8601 na 'HH:MM:SS', dodaje Video_Type
    """

    video_id = row.get('Video_ID') or row.get('video_id')
    raw_duration = row.get('Duration')

    logger.info(
        f"Transform start | video_id={video_id} | raw_duration={raw_duration}"
    )

    try:
        duration_td = parse_duration(raw_duration)
    except Exception as e:
        logger.error(
            f"Duration parse FAILED | video_id={video_id} | "
            f"raw_duration={raw_duration} | error={e}"
        )
        raise

    total_sec = int(duration_td.total_seconds())

    hours, remainder = divmod(total_sec, 3600)
    minutes, seconds = divmod(remainder, 60)

    formatted_duration = f"{hours:02}:{minutes:02}:{seconds:02}"

    logger.info(
        f"Transform OK | video_id={video_id} | "
        f"parsed_duration={formatted_duration} | total_sec={total_sec}"
    )

    row['Duration'] = formatted_duration
    row['Video_Type'] = 'Shorts' if total_sec <= 60 else 'Normal'

    return row