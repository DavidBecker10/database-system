# Entidade para o banco de dados

import os
from file import File
from queryProcessor import QueryProcessor

class Database:

    def __init__(self):
        self.file = File()
        self.tables = []
        self.QP = QueryProcessor()
        self.start()

    def start(self):
        sel = self.readInput()
        while sel != '4':
            match sel:
                case '1':
                    self.importTable()
                case '2':
                    self.query()
                case '3':
                    self.showTables()
            sel = self.readInput()

    def readFile(self, csvPath):
        print(csvPath)
        self.file.read(csvPath, sep = ';')

    def readInput(self):
        sel =  input('------- Menu -------\n1- Importar arquivo\n2- Fazer Queries \n3- Mostrar tabelas\n4- Sair\n')
        while sel not in ['1','2','3','4']:
            sel =  input('------- Menu -------\n1- Importar arquivo\n2- Fazer Queries \n3- Mostrar tabelas\n4- Sair\n')
        return sel

    def importTable(self):
        self.csvPath = input('Digite o caminho do arquivo csv da tabela a ser importada: \n')
        self.QP.setPath(self.csvPath)
        try:
            # Le arquivo csv, cria uma Table e passa para o queryProcessor
            table = self.file.read(self.csvPath,sep=';')
            self.tables.append(table)
            self.QP.setTable(table)
        except:
            print('Erro na leitura, tente novamente.')

    def showTables(self):
        if(len(self.tables)):
            print("\n--- Tabelas importadas ---")
            for i in range(len(self.tables)):
                print(self.tables[i].getName())
            print("\n")
        else:
            print("Nenhuma tabela foi importada.")

    def query(self):
        sel = ''
        while sel != '2':
            self.clearTerminal()
            query = input('Digite a query: \n')
            try:
            # Processando a query
                self.QP.setQuery(query)
                self.QP.process()
                self.reset()
            except:
               print('Erro na consulta, tente novamente.')
            
            sel = input('Gostaria de fazer mais uma consulta?\n1-Sim\n2-Não\n')

        self.clearTerminal()

    def reset(self):
        self.QP.whereResult = None

    def clearTerminal(self):
        os.system('cls' if os.name == 'nt' else 'clear')