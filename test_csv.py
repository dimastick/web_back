#!/usr/bin/env python3

import csv
from collections import OrderedDict

FIELDS = (
    "id", "name_soname", "date", "year", "sex", "city", "school", "club", "competition", "comp_date",
    "comp_location", "event_type", "result", "position", "scores", "scores_2", "scores_3",
    "scores_4", "scores_5", "scores_total", "trainer_name_1", "trainer_name_2"
)

with open('../data_2018.csv', newline='') as f:
    has_header = csv.Sniffer().has_header(f.read(1024))     # check if header exists by sample
    f.seek(0)                                               # seting the file's current position at 0
    dialect = csv.Sniffer().sniff(f.read(1024))
    f.seek(0)
    reader = csv.DictReader(f, fieldnames=FIELDS, dialect=dialect)
    if has_header:
        next(reader, None)                                  # to exclude the first row (headers)
    for row in reader:
        print(row)