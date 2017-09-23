# -*- coding: utf-8
# show.py

import glob
import os
import sys
import simplejson as json
from pprint import pprint

from context import OUTPUT_PATH


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: {} <COUNT>'.format(sys.argv[0]))
        sys.exit(0)

    requested = int(sys.argv[1])
    json_files = glob.glob1(OUTPUT_PATH, '*.json')
    file_name = os.path.join(OUTPUT_PATH, json_files[0])

    collected, total = 0, 0
    with open(file_name, 'r') as f:
        for line in f:
            if collected < requested:
                data = json.loads(line)
                pprint(data)
                print("")
                collected += 1
            total += 1

    print('-' * 20)
    print('Total: {}'.format(total))
