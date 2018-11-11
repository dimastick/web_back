#!/usr/bin/env python3
import os
import re
import sys
import queries
import getopt
from MySQLdb import connections
import SelectResult

COLUMNS = (
    "name_soname", "date", "year", "sex", "city", "school", "club", "competition", "comp_date",
    "comp_location", "event_type", "result", "position", "scores", "scores_2", "scores_3",
    "scores_4", "scores_4", "scores_total", "trainer_name_1", "trainer_name_2"
)

trainer_NAME_COL_FORMAT = [
    r'^[А-Я][a-я]*(\s[А-Я]{1}\.?)?$',
    r'^[А-Я][a-я]*\s[А-Я]{1}\.\s?[А-Я]{1}\.$',
    r'^[А-Я][a-я]*\s[А-Я]{1}\s[А-Я]{1}$',
    r'^(н/д|б/т)$',
]

COLFORMATS = dict(
    name_soname=[
        r'^[А-Я][a-я]* [А-Я][a-я]*$',
        r'^[А-Я][a-я]*-[А-Я][a-я]* [А-Я][a-я]*$'
    ],
    sex=[r'(^муж$|^жен$)'],
    date=[
        r'^\d{2}.\d{2}.\d{4}$',
        r'^$'
    ],
    trainer_name_1=trainer_NAME_COL_FORMAT,
    trainer_name_2=trainer_NAME_COL_FORMAT
)

SYS_USER = os.getenv("USER") # get system user
CONFIG = {
    'user': 'dimasty' if SYS_USER == 'dimasty' else 'root',
    'password': 'dimasty' if SYS_USER == 'dimasty' else '',
    'host': 'localhost',
    'database': 'stat'
}

db = connections.Connection(**CONFIG)
dbc = db.cursor()
db.set_character_set('utf8')
dbc.execute('SET NAMES utf8;')
dbc.execute('SET CHARACTER SET utf8;')
dbc.execute('SET character_set_connection=utf8;')


class InputData(object):
    """prepare/parse csv file with data separated by "\" """

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
            record_obj.check_col_format(COLFORMATS)
            self.isCheckDone = True
            if not record_obj.isRecordValid:
                self.isCheckPassed = False
        return self

    def getdata_byrow(self):
        if self.isCheckDone and self.isCheckPassed:
            # After data check we need to rewind the file to start iteration again
            self.fh.seek(0)
            generator = (rec for rec in map(
                lambda line: line.split("|"), self.fh))
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


class DataRecord(object):
    """Represents one Stat record"""

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

    def _get_trainers(self):
        trainers = [s.strip() for s in self.r[-1].split(",")]
        return trainers

    def devide_lastcol(self):
        trainers = self._get_trainers()
        if len(trainers) == 3:
            print("[WARN] 3 trainers are encountered")
            sys.exit()
        elif len(trainers) == 2:
            self.r.pop()
            if trainers[0] == '':  # replacing "" with "н/д"
                trainers[0] = 'н/д'
            self.r.extend(trainers)
        elif len(trainers) == 1:
            self.r.pop()
            if trainers[0] == '':  # replacing "" with "н/д"
                trainers[0] = 'н/д'
            trainers.append('н/д')
            self.r.extend(trainers)
        else:
            self.r.pop()
            self.r.extend(*['н/д', 'н/д'])

    def add_year_to_date(self):
        """Add 01.01 if dd.mm cell is empty and concatenate it with yyyy cell
        first column of csv file should be remove first"""
        if not self.isFirstColDeleted:
            print(
                "First col is not delete. There might be error because of wrong indexing")
            exit()
        if not self.r[1]:
            self.r[1] = "01.01"
        self.r[1] = self.r[1] + "." + self.r[2]

    def check_col_format(self, regexps):
        """Checking record values by provided regexps from col_formats"""
        if len(COLUMNS) != len(self.r):
            sys.exit("[Error] Can't do correct mapping of cell names onto their "
                     "values. Count of column titles differs from count of values")
        named_cells = dict(zip(COLUMNS, self.r))
        search_result = dict()
        for key in COLFORMATS:
            search_result[key] = dict()
            for regexp in regexps[key]:
                search_obj = re.search(regexp, named_cells[key])
                search_result[key][regexp] = search_obj.string if search_obj else search_obj
            if not any(search_result[key].values()):
                self.isRecordValid = False
                print("[{0}] does not match format set for {1}. Please check records with [{2}]"
                      .format(named_cells[key], key, named_cells["name_soname"].strip()))
        if named_cells["trainer_name_1"] == named_cells["trainer_name_2"]:
            if named_cells["trainer_name_1"] != 'н/д':
                self.isRecordValid = False
                print("[WARN] the first trainer is [{0}] and the second one is [{1}]. "
                      "Please check records with [{2}]"
                      .format(
                          named_cells["trainer_name_1"],
                          named_cells["trainer_name_2"],
                          named_cells["name_soname"].strip()
                      )
                      )
        if named_cells["trainer_name_2"] != 'н/д' and named_cells["trainer_name_1"] == 'н/д':
            self.isRecordValid = False
            print("The second trainer [{0}] but the first one is [{1}]"
                  .format(named_cells["trainer_name_2"], named_cells["trainer_name_1"],))
        # print(json.dumps(d, ensure_ascii=False))
        # sys.exit()
        return self


def usage():
    print("\nThis is the usage function\n")
    print("Usage: " + sys.argv[0] + " -f <file> or --data-file=<file>]")

def get_duplicate_name():
    try:
        dbc.execute(queries.FIND_PERSON_DUPLICATES)
    except connections.Error as e:
        print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
    return dbc


def get_regdata_by_name(participant):
    try:
        dbc.execute(queries.GetPersonalInfoByName, {"name": participant})
    except connections.Error as e:
        print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
    return dbc


try:
    opts, args = getopt.getopt(sys.argv[1:], "dhf:", [
                               "help", "data-file=", "by-name="])
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
            dbc.execute(queries.CLEAR_TRAINERS_TABLE)
            with open(file) as f:
                dbc.executemany(queries.addRecordFromFile, InputData(
                    f).checkRecordsFormat().getdata_byrow())
                dbc.execute(queries.FillInAthletesTable)
                dbc.execute(queries.AddAthleteIdToStatRecord)
                dbc.execute(queries.FILL_IN_TRAINERS_TABLE)
        except connections.Error as e:
            print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
        else:
            db.commit()
    elif o in ("-d",):
        q_object = get_duplicate_name()
        SelectResult.SelectResult(q_object).print_qresult()
    elif o in ("--by-name"):
        q_object = get_regdata_by_name(a)
        result = SelectResult.SelectResult(q_object).print_qresult().report_tofile("duplicates.csv")
    else:
        assert False, "unhandled option"
db.close()
