#!/usr/bin/env python3
import os
import re
import sys
import queries
import getopt
import MySQLdb
from SelectResult import SelectResult

columns = ("name_soname", "date", "year", "sex", "city", "school", "club", "competition", "comp_date",
           "comp_location", "event_type", "result", "position", "scores", "scores_2", "scores_3",
           "scores_4", "scores_4", "scores_total", "teacher_name")

col_formats = dict(name_soname=[r'^[А-Я][a-я]* [А-Я][a-я]*$', r'^[А-Я][a-я]*-[А-Я][a-я]* [А-Я][a-я]*$'],
                   sex=[r'(^муж$|^жен$)'],
                   date=[r'^\d{2}.\d{2}.\d{4}$', r'^$'])

db = MySQLdb.connect("localhost", "dimasty", "dimasty", "stat")
dbc = db.cursor()
db.set_character_set('utf8')
dbc.execute('SET NAMES utf8;')
dbc.execute('SET CHARACTER SET utf8;')
dbc.execute('SET character_set_connection=utf8;')


# prepare/parse csv file with data separated by "\"
class InputData:
    def __init__(self, file_obj):
        self.fh = file_obj
        self.isCheckPassed = True
        self.isCheckDone = False

    def checkRecordsFormat(self):
        generator = (rec for rec in map(lambda line: line.split("|"), self.fh))
        next(generator)
        for r in generator:
            record_obj = DataRecord(r)
            record_obj.del_firstvalue().del_newline_char()
            record_obj.devide_lastcol()
            record_obj.add_year_to_date()
            record_obj.check_col_format(col_formats)
            self.isCheckDone = True
            if not record_obj.isRecordValid:
                self.isCheckPassed = False
        return self

    def getdata_byrow(self):
        if self.isCheckDone and self.isCheckPassed:
            self.fh.seek(0) # After data check we need to rewind the file to start iteration again
            generator = (rec for rec in map(lambda line: line.split("|"), self.fh))
            next(generator)
            for r in generator:
                record_obj = DataRecord(r)
                record_obj.del_firstvalue().del_newline_char()
                record_obj.devide_lastcol()
                record_obj.add_year_to_date()
                yield tuple(r)
        elif not self.isCheckPassed:
            print("Data check was not passed")
            exit()
        else:
            print("Data check was not Done")
            exit()


class DataRecord:
    def __init__(self, record):
        self.r = record
        self.isFirstColDeleted = False
        self.isRecordValid = True

    def get_record(self):
        return self.r

    def del_firstvalue(self):
        self.r.pop(0)
        self.isFirstColDeleted = True
        return self

    def del_newline_char(self):
        self.r[len(self.r) - 1] = self.r[len(self.r) - 1].replace("\n", "")
        return self

    def _get_teachers(self):
        teachers = [s.strip() for s in self.r[-1].split(",")]
        return teachers

    def devide_lastcol(self):
        ts = self._get_teachers()
        if len(ts) == 3:
            print("[WARN] 3 teachers are encountered")
            exit()
        elif len(ts) == 2:
            self.r.pop()
            self.r.extend(ts)
        elif len(ts) == 1:
            self.r.pop()
            ts.append("")
            self.r.extend(ts)
        else:
            self.r.pop()
            self.r.extend(*["", ""])

    def add_year_to_date(self):
        if not self.isFirstColDeleted:
            print("First col is not delete. There might be error because of wrong indexing")
            exit()
        if not self.r[1]:
            self.r[1] = "01.01"
        self.r[1] = self.r[1] + "." + self.r[2]

    def check_col_format(self, regexps):
        named_cells = dict(zip(columns, self.r))
        for key in col_formats:
            match_obj = None
            is_matched = False
            for regexp in regexps[key]:
                match_obj = re.search(regexp, named_cells[key])
                is_matched = False if (not is_matched and not match_obj) else True
            if not is_matched:
                self.isRecordValid = False
                print("[{0}] does not match format. Please see records with [{1}]"
                      .format(named_cells[key], named_cells["name_soname"].strip()))
        return self


def usage():
    print("\nThis is the usage function\n")
    print("Usage: " + sys.argv[0] + " -f <file> or --data-file=<file>]")


def get_duplicate_name():
    try:
        dbc.execute(queries.findPersoneDuplicate)
    except MySQLdb.Error as e:
        print("MySQL Error {0}: {1}" % (e.args[0], e.args[1]))
    return dbc


def get_regdata_by_name(participant):
    try:
        dbc.execute(queries.GetPersonalInfoByName, {"name": participant})
    except MySQLdb.Error as e:
        print("MySQL Error {0}: {1}" % (e.args[0], e.args[1]))
    return dbc


try:
    opts, args = getopt.getopt(sys.argv[1:], "dhf:", ["help", "data-file=", "by-name="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
for o, a in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit()
    elif o in ("-f", "--data-file"):
        file = a
        if not os.path.isfile(file):
            print('argument is not a file')
            sys.exit()
        try:
            dbc.execute(queries.ClearRecordsTable)
            dbc.execute(queries.ClearAthletesTable)
            with open(file) as f:
                dbc.executemany(queries.addRecordFromFile, InputData(f).checkRecordsFormat().getdata_byrow())
                dbc.execute(queries.FillInAthletesTable)
                dbc.execute(queries.AddAthleteIdToStatRecord)
        except MySQLdb.Error as e:
            print("MySQL Error {0}: {1}" % (e.args[0], e.args[1]))
        else:
            db.commit()
    elif o in ("-d",):
        q_object = get_duplicate_name()
        SelectResult(q_object).print_qresult()
    elif o in ("--by-name"):
        q_object = get_regdata_by_name(a)
        result = SelectResult(q_object).print_qresult().report_tofile("duplicates.csv")
    else:
        assert False, "unhandled option"
db.close()