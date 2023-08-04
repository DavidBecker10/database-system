# Leitor de arquivo CSV

import csv
from field import Field
from table import Table

class File():
    
    def __init__(self):
        pass
    
    def read(self, path:str, sep = ';'):
        csvfile = open(path, newline='')
        tableName = path.split('\\')[-1][:-4] # Pegando o nome do arquivo (tabela)
        records = csv.reader(csvfile, delimiter=';')
        fields = next(records)
        table = Table(tableName, fields, records)
        return table
    
    def getFirstLine(self, path):
        with open(path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            firstLine = next(reader)  # Obtém a primeira linha usando a função next()  
        # Tratamento do formato
        split_line = firstLine[0].split(';')
        split_line = [element.strip('"') for element in split_line]
        return split_line
    
    def formatReg(self, path, fields, elements):
        # Com as posições das colunas que tenho posso preencher só esses espaços
        new = []
        firstLine = self.getFirstLine(path)
        i = 0
        for field in firstLine:
            if field in fields:
                new.append(elements[i])
                i += 1
            else:
                new.append('')
        return new

    ##################### Tratando INSERT #####################
    def insert(self, path, fields, elements): # fields tem as colunas e elements os valores
        new = self.formatReg(path, fields, elements)
        try:
            print(path)
            with open(path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=';')
                writer.writerow(new)
        except Exception as e:
            print("Erro ao inserir linha no arquivo:", str(e))

    ##################### Tratando DELETE #####################
    def delete(self, path, listRemove):
        # Abre o arquivo CSV em modo de leitura
        with open(path, 'r', newline='') as csvfile:
            readercsv = csv.reader(csvfile)
            regs = list(readercsv)
        
        # Remove os registros com base no critério
        #regsFilter = [linha for linha in regs if linha not in listRemove]
        formatRegs = []
        for reg in regs:
            reg = reg[0].split(';')
            reg = [element.strip('"') for element in reg]
            formatRegs.append(reg)
        
        for index in sorted(listRemove, reverse=True):
            del formatRegs[index]

        # Abre o arquivo CSV novamente em modo de escrita
        with open(path, 'w', newline='') as arquivo_csv:
            writer = csv.writer(arquivo_csv, delimiter=';')
            writer.writerows(formatRegs)

    ##################### Tratando UPDATE #####################
    def update(self, path, posRegs, newValues):
         # Abre o arquivo CSV em modo de leitura
        with open(path, 'r', newline='') as csvfile:
            readercsv = csv.reader(csvfile)
            rows = list(readercsv)
        
        # Atualizar os registros com base nas posições e valores fornecidos
        for pos, value in zip(posRegs, newValues):
            rows[pos] = value
            
        # Abrir o arquivo CSV novamente em modo de escrita
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)