import json

from cravat.cravat_report import CravatReport
import sys
import datetime
import re
import csv
import zipfile
from pathlib import Path
import sqlite3
from mako.template import Template

class Reporter(CravatReport):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.savepath = kwargs["savepath"]


    async def run(self):
        self.setup()
        self.write_data()
        self.end()
        pass


    def setup(self):
        self.input_database_path = f'{self.savepath}_superhuman.sqlite'
        self.db_conn = sqlite3.connect(self.input_database_path)
        self.db_cursor = self.db_conn.cursor()
        outpath = f'{self.savepath}.superhuman.html'
        self.outfile = open(outpath, 'w', encoding='utf-8')
        self.template = Template(filename=str(Path(__file__).parent / "template.html"))


    def write_table(self, name, sort_field = "", sort_revers = False):
        try:
            sort_sql = ""
            if sort_field != "":
                sort_sql = " ORDER BY " + sort_field
                if sort_revers:
                    sort_sql = sort_sql+" DESC"
                else:
                    sort_sql = sort_sql+" ASC"

            self.db_cursor.execute("SELECT * FROM "+name+sort_sql)
            rows = self.db_cursor.fetchall()
            res = []

            for row in rows:
                tmp = {}
                for i, item in enumerate(self.db_cursor.description):
                    tmp[item[0]] = row[i]
                res.append(tmp)
            return res
        except Exception as e:
            print("Warning:", e)

    def write_data(self):
        # self.data = {"test1":[1,2,3], "test2":["aa", "bbb", "cccc"]}
        data = {}
        data["superhuman"] = self.write_table("superhuman", "id", True)
        print(data)
        self.outfile.write(self.template.render(data=data))


    def end(self):
        self.outfile.close()
        return Path(self.outfile.name).resolve()


### Don't edit anything below here ###
def main():
    reporter = Reporter(sys.argv)
    reporter.run()


if __name__ == '__main__':
    main()