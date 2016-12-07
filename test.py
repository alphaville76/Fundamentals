from click import progressbar
import time
import sys

row_count = 30
with progressbar(length=row_count) as progress:
    for i in range(row_count):
        time.sleep(1)
        progress.update(1)
        progress.label='%10d of %10d' % (progress.pos, progress.length)