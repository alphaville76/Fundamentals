import os


def get_start():
    try:
        with open('resource/progress.txt', 'r') as progress_file:
            return int(progress_file.read()) - 1
    except IOError:
        print("progress.txt doesn't exists")

    return 0


def to_float(row, key=None):
    if key is None:
        return None if not row else float(row)

    s = row[key]
    try:
        return None if not s else float(s)
    except ValueError:
        print("could not convert '%s' to float (key: %s, ticker: %s, datekey: %s): " % (s, key, row['ticker'], row['datekey']))


def update_progress(progress):
    progress.update(1)
    progress.label = '%8d of %d' % (progress.pos, progress.length)
    with open('resource/progress.txt', 'w') as progress_file:
        progress_file.write(str(progress.pos))

def delete_progress_file():
    os.remove('resource/progress.txt')