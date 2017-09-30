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
    file_name = os.path.join(json_path, json_files[0])

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
