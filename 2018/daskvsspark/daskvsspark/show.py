# -*- coding: utf-8
# show.py

import glob
import os
import sys
import simplejson as json
from pprint import pprint


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: {} <PATH> <COUNT>'.format(sys.argv[0]))
        sys.exit(0)

    json_path = sys.argv[1]
    requested = int(sys.argv[2])
    json_files = glob.glob1(json_path, '*.json')

    collected, total = 0, 0
    for file_name in json_files:
        full_name = os.path.join(json_path, file_name)
        with open(full_name, 'r') as f:
            for line in f:
                if collected < requested:
                    data = json.loads(line)
                    pprint(data)
                    print("")
                    collected += 1
                total += 1

    print('-' * 20)
    print('Total files: {}'.format(len(json_files)))
    print('Total lines: {}'.format(total))
