# Cria entidade tabela que guarda os registros
 
from field import Field

class Table():

    def __init__(self, name:str, fieldsName:list = [], records:list = []):
        self.fields = []
        self.name = name
        for i, Fldname in enumerate(fieldsName):
            self.fields.append(Field(Fldname))
        
        for row in records:
            for i, value in enumerate(row):
                self.fields[i].insertRecord(value)
    
    def getName(self):
        return self.name

    def getRow(self, row):
        rowRegs = []
        for field in self.fields:
            rowRegs.append(field.getRecord(row))
        return rowRegs

    def getFieldsName(self):
        fieldsName = []
        for field in self.fields:
            fieldsName.append(field.getName())
        return fieldsName
    
    def getFieldRecords(self, fieldName, rows):
        for field in self.fields:
            if(fieldName == field.getName()):
                fieldsRecords = field.getRecords(rows)
        
        if(not fieldsRecords):
            return None
        
        return fieldsRecords
    
    def setFieldRecords(self, fieldName, records):
        for field in self.fields:
            if field.getName() == fieldName:
                field.setRecords(records)

    def print(self):
        fieldsNames = []
        fieldsRecords = []

        for index, field in enumerate(self.fields):
            fieldsNames.append(field.getName())
            if field.getRecords(None):
                fieldsRecords.append(field.getRecords(None))
        
        print()
        
        print('|'.join(format(col, "^15") for col in fieldsNames))
        for i in range(len(fieldsNames)*15):
            print("-", end="")
        
        print()

        if(fieldsRecords):
            for i in range(len(fieldsRecords[0])):
                row = []
                for j in range(len(fieldsRecords)):
                    row.append(''.join(fieldsRecords[j][i]))
                if row:
                    print('|'.join(format(col, "^15") for col in row))
            
            print()