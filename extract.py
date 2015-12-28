# -*- coding: utf-8 -*-
__author__ = 'jude'

import csv
import json
raw_data = json.load(open("data/yummy/train.json"))
writer = csv.writer(open('data/yummy/extract.csv', 'w+'))
for item in raw_data:
    writer.writerow([item[u'cuisine']]+[unicode(i).encode('utf8') for i in item[u'ingredients']])