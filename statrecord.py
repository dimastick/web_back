#!/usr/bin/env python3

import re
import sys


class StatRecord:
    """Represents one Stat record"""

    def __init__(self, record):
        self.r = record
        self.isRecordValid = True

    def get_record(self):
        return self.r

    def fix_it_up(self):
        """
            Divide trainer_name_1 on two fields if there are two trainers;
            Add 01.01 if dd.mm (date) value is empty and concatenate it with year field;
            @return: fixed (modified) dict
        """
        del self.get_record()['id'] # delete the first column field

        trainers = [s.strip() for s in self.r['trainer_name_1'].split(",")]
        if len(trainers) == 3:
            print("[WARN] 3 trainers are encountered for {}".format(self.r['name_soname']))
            sys.exit()
        elif len(trainers) == 2:
            if trainers[0] == trainers[1]:
                print("[WARN] two identical trainer names are encountered in one record")
                sys.exit()
            if not trainers[0] or not trainers[1]:
                print("[WARN] one of two trainer names is empty for {}".format(self.r['name_soname']))
                sys.exit()
            self.r['trainer_name_1'], self.r['trainer_name_2'] = trainers
        elif len(trainers) == 1:
            trainers.append('н/д')
            self.r['trainer_name_1'], self.r['trainer_name_2'] = trainers

        # if date value is empty set it to "01.01" and add "year" value to it
        self.r['date'] = self.r['date'] if self.r['date'] else "01.01"
        self.r['date'] = ".".join([self.r['date'], self.r['year']])

    def check_col_format(self, regexps):
        """Checking record values by provided regexps from col_formats"""
        search_result = dict()
        for key in regexps:
            search_result[key] = dict()
            for regexp in regexps[key]:
                search_obj = re.search(regexp, self.r[key])
                search_result[key][regexp] = search_obj.string if search_obj else search_obj
            if not any(search_result[key].values()):
                self.isRecordValid = False
                print("""[{0}] does not match [{1}] field format. Please check records with [{2}]"""
                      .format(self.r[key], key, self.r["name_soname"]))