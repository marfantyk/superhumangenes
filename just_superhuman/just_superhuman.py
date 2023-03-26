from oakvar import BasePostAggregator
from pathlib import Path
import sqlite3

class CravatPostAggregator (BasePostAggregator):
    sql_insert:str = """ INSERT INTO superhuman (
            gene,
            rsid,
            ref,
            alt,
            genotype,
            zygosity,
            superability,
            adv_effects,
            refer,
            clinvarid,
            omimid,
            ncbi
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) """

    def get_nucleotides(self, ref:str, alt:str, zygocity:str) -> str:
        if zygocity == 'hom':
            return alt+"/"+alt
        return alt+"/"+ref

    def check(self):
        return True

    def cleanup(self):
        if self.superhuman_cursor is not None:
            self.superhuman_cursor.close()
        if self.superhuman_conn is not None:
            self.superhuman_conn.commit()
            self.superhuman_conn.close()
        return

    def setup (self):
        self.result_path = Path(self.output_dir, self.run_name + "_superhuman.sqlite")
        self.superhuman_conn = sqlite3.connect(self.result_path)
        self.superhuman_cursor = self.superhuman_conn.cursor()
        sql_create:str = """ CREATE TABLE IF NOT EXISTS superhuman (
            id integer NOT NULL PRIMARY KEY,
            gene text,
            rsid text,
            ref text,
            alt text,
            genotype text,
            zygosity text,
            superability text,
            adv_effects text,
            refer text,
            clinvarid text,
            omimid text,
            ncbi text
            )"""
        self.superhuman_cursor.execute(sql_create)
        self.superhuman_cursor.execute("DELETE FROM superhuman;")
        self.superhuman_conn.commit()


        cur_path:str = str(Path(__file__).parent)
        self.data_conn:sqlite3.Connection = sqlite3.connect(Path(cur_path, "data", "superhuman.sqlite"))
        self.data_cursor:sqlite3.Cursor = self.data_conn.cursor()


    def annotate(self, input_data):
        rsid:str = str(input_data['dbsnp__rsid'])
        if rsid == '':
            return

        if not rsid.startswith('rs'):
            rsid = "rs" + rsid

        zygot:str = input_data['vcfinfo__zygosity']
        if zygot is None or zygot == "":
            zygot = "hom"

        alt:str = input_data['base__alt_base']
        ref:str = input_data['base__ref_base']
        genotype:str = self.get_nucleotides(ref, alt, zygot)

        query:str = 'SELECT * FROM superhuman WHERE rsid = ? AND (zygosity = ? OR zygosity = "both") AND (alt_allele = ? OR alt_allele IS NULL)'
        args:tuple[str, ...] = (rsid, zygot, input_data['base__alt_base'])
        self.data_cursor.execute(query, args)
        rows:tuple = self.data_cursor.fetchone()

        if rows is None:
            return None

        task:tuple[str, ...] = (rows[1], input_data['dbsnp__rsid'], ref,
                alt, genotype, zygot, rows[8], rows[9], rows[10],
                input_data['clinvar__id'], input_data['omim__omim_id'],
                input_data['ncbigene__ncbi_desc'])

        self.superhuman_cursor.execute(self.sql_insert, task)
        return {"col1":""}