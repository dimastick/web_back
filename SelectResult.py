from functools import reduce
import csv
import os


class SelectResult:
    def __init__(self, cursor_obj):
        self.cursor = cursor_obj
        self.titles = tuple([col_meta_data[0] for col_meta_data in self.cursor.description])
        self.data = self.cursor.fetchall()
        self.data_toCSV = list(self.data)

    def report_tofile(self, filename):
        fullpath_tofile = "/home/" + os.getlogin() + "/PycharmProjects/stat/output/" + filename
        writer = csv.writer(open(fullpath_tofile, "w"), delimiter="|", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(self.titles)
        # Query result is returned by records. Writing each record field by field
        for q_rec in self.data_toCSV:
            writer.writerow([field for field in q_rec])

    def get_titles(self):
        return self.titles

    def get_data(self):
        return self.data

    def _get_maxfields_len(self):
        max_field_size = []
        data = self.data
        t = self.titles
        if len(data[0]) != len(t):
            print(">>> number of columns not equal to number of titles")
        for i in range(len(t)):
            max_field_value_len = max([len(row[i].__str__()) for row in data])
            max_fvalue_title = max(max_field_value_len, len(t[i]))
            max_field_size.append((t[i], max_fvalue_title))
        return max_field_size

    def _print_dashes(self):
        indent = 2
        max_flen = self._get_maxfields_len()
        print("+", end='')
        for f_len in max_flen:
            print("".ljust(f_len[1] + indent, "-") + "+", end="")
        print()

    def _print_row(self):
        indent = 2
        max_flen = self._get_maxfields_len()
        for line in self.data:
            print("|", end="")
            m = map(lambda x, y: (x.__str__().ljust(y[1] + indent)) + "|", line, max_flen)
            r = reduce(lambda x, y: x + y, m)
            print(r)

    def _print_title(self):
        indent = 2
        max_flen = self._get_maxfields_len()
        t = self.titles
        print("|", end="")
        m = map(lambda x, y: (x.__str__().ljust(y[1] + indent)) + "|", t, max_flen)
        r = reduce(lambda x, y: x + y, m)
        print(r)

    def print_qresult(self):
        self._print_dashes()
        self._print_title()
        self._print_dashes()
        self._print_row()
        self._print_dashes()
        return self
