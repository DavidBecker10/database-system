# Entidade campo que armazena as informações de uma coluna

class Field:

    def __init__(self, name:str):
        self.name = name
        self.records = []

    def getName(self):
        return self.name

    def getRecord(self, row):
        return self.records[row]

    def getRecords(self, rows):
        rec = []
        
        if(rows != None):
            for row in rows:
                rec.append(self.records[row])
        else:
            for row in self.records:
                rec.append(row)
        return rec
    
    def setRecords(self, records):
        self.records = records
    
    def insertRecord(self, record):
        self.records.append(record)

    def printRecords(self):
        print(next(self.records))