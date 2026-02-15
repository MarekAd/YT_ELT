from datetime import timedelta
import logging
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

def parse_duration(duration_str: str) -> timedelta:
    """
    Zamienia ISO 8601 duration (np. 'PT37S', 'PT23M31S', 'P1DT2H3M4S') na timedelta
    """
    if not duration_str.startswith("P"):
        raise ValueError("Invalid ISO 8601 duration")
    
    pattern = re.compile(
        r'P'                     # starts with P
        r'(?:(?P<days>\d+)D)?'   # days
        r'(?:T'                  # time part
        r'(?:(?P<hours>\d+)H)?'  
        r'(?:(?P<minutes>\d+)M)?'
        r'(?:(?P<seconds>\d+)S)?'
        r')?'
    )
    
    match = pattern.fullmatch(duration_str)
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration: {duration_str}")
    
    days = int(match.group('days') or 0)
    hours = int(match.group('hours') or 0)
    minutes = int(match.group('minutes') or 0)
    seconds = int(match.group('seconds') or 0)
    
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def transform_data(row):
    """
    Zamienia row['Duration'] z ISO 8601 na 'HH:MM:SS', dodaje Video_Type
    """

    video_id = row.get('Video_ID') or row.get('video_id')
    raw_duration = row.get('Duration')

    logger.debug(
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

    logger.debug(
        f"Transform OK | video_id={video_id} | "
        f"parsed_duration={formatted_duration} | total_sec={total_sec}"
    )

    row['Duration'] = formatted_duration
    row['Video_Type'] = 'Shorts' if total_sec <= 60 else 'Normal'

    return row