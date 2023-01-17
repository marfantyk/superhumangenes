from oakvar import BasePostAggregator
from pathlib import Path
import sqlite3

class CravatPostAggregator (BasePostAggregator):
    genes:set[str] = set()
    significance_filter:list[str] = [
        "Pathogenic",
        "Pathogenic/Likely pathogenic"
    ]

    def check(self):
        return True

    def setup (self):
        with open(str(Path(__file__).parent)+"/data/genes.txt") as f:
            self.genes = set(f.read().split("\n"))
        self.result_path:str = Path(self.output_dir, self.run_name + "_superhuman.sqlite")
        self.longevity_conn:str = sqlite3.connect(self.result_path)
        self.longevity_cursor:str = self.longevity_conn.cursor()
        # Sequence ontology - base__so
        sql_create:str = """ CREATE TABLE IF NOT EXISTS superhuman (
            id integer NOT NULL PRIMARY KEY,
            gene text,
            rsid text,
            cdnachange text,
            genotype text,
            sequence_ontology text,
            sift_pred text,
            alelfreq text,
            phenotype text,
            significance text,
            clinvarid text,
            omimid text,
            ncbi text
            )"""
        self.longevity_cursor.execute(sql_create)
        self.longevity_cursor.execute("DELETE FROM superhuman;")
        self.longevity_conn.commit()


    def cleanup(self):
        if self.longevity_cursor is not None:
            self.longevity_cursor.close()
        if self.longevity_conn is not None:
            self.longevity_conn.commit()
            self.longevity_conn.close()

        return

    def get_nucleotides(self, ref:str, alt:str, zygocity:str) -> str:
        if zygocity == 'hom':
            return alt+"/"+alt
        return alt+"/"+ref

    def annotate(self, input_data):
        gene:str = input_data['base__hugo']
        if gene not in self.genes:
            return

        sql:str = """ INSERT INTO superhuman (
            gene,
            rsid,
            cdnachange,
            genotype,
            sequence_ontology,
            sift_pred,
            alelfreq,
            phenotype,
            significance,
            clinvarid,
            omimid,
            ncbi
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) """

        zygot:str = input_data['vcfinfo__zygosity']
        if zygot is None or zygot == "":
            zygot = "het"

        alt:str = input_data['base__alt_base']
        ref:str = input_data['base__ref_base']
        genotype:str = self.get_nucleotides(ref, alt, zygot)

        task:tuple[str, ...] = (gene, input_data['dbsnp__rsid'], input_data['base__cchange'],
                genotype, input_data['base__so'], "",
                input_data['gnomad__af'], input_data['clinvar__disease_names'], "",
                input_data['clinvar__id'], input_data['omim__omim_id'],
                input_data['ncbigene__ncbi_desc'])

        self.longevity_cursor.execute(sql, task)
        return {"col1":""}