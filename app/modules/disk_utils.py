import logging
import os
import shutil

import humanfriendly

log = logging.getLogger(__name__)


def check_disk_full(*paths, minimum="50MB"):
    bytes_size = humanfriendly.parse_size(minimum) if isinstance(minimum, str) else minimum

    for path in [os.getcwd(), *paths]:
        total, used, free = shutil.disk_usage(path)
        totalh, usedh, freeh = humanfriendly.format_size(total), humanfriendly.format_size(used), humanfriendly.format_size(free)
        # print(f"total: {totalh}, used: {usedh}, free:{freeh}")

        if free < bytes_size:
            log.warning(f"Disk is running out of space path: {path} total: {totalh}, used: {usedh}, free:{freeh}")
            return True

    return False
