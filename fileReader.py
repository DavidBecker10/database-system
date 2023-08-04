# Leitor de arquivo CSV

import csv
from field import Field
from table import Table

class FileReader():
    
    def __init__(self):
        pass
    
    def read(self, path:str, sep = ';'):
        csvfile = open(path, newline='')
        tableName = path.split('\\')[-1][:-4] # Pegando o nome do arquivo (tabela)
        records = csv.reader(csvfile, delimiter=';')
        fields = next(records)
        table = Table(tableName, fields, records)
        return table