from datetime import datetime
import re


def check_log_interval(filename='IntercomRecorder.log'):
    with open(filename, 'r') as f:
        data = f.readlines()
        for i in range(1, len(data)):
            if (after := re.search(r'(?<=^\[).+?(?=\])', data[i])) and\
                    (before := re.search('(?<=^\[).+?(?=\])', data[i-1])):
                if re.search(r'[A-z]', after.group(0)) or re.search(r'[A-z]', before.group(0)):
                    continue
                after_dt = datetime.strptime(after.group(0).split(',')[0], '%Y-%m-%d %H:%M:%S')
                before_dt = datetime.strptime(before.group(0).split(',')[0], '%Y-%m-%d %H:%M:%S')
                if (gap := (after_dt - before_dt).seconds) > 121:
                    print(f'{gap=} seconds; BEFORE="{before_dt}" AFTER="{after_dt}"')

if __name__ == '__main__':
    check_log_interval()
