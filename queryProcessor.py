from queryAnalyzer import QueryAnalyzer
from table import Table
from file import File

class QueryProcessor():

    def __init__(self):
        self.QA = QueryAnalyzer()
        self.tables = []
        self.paths = []
        self.whereResult = None
        self.orderByFinalResult = None
        self.indexMainTable = -1
        self.indexJoinTable = -1
        self.query = str
        self.file = File()
    
    def setQuery(self, query):
        self.query = query.lower()
        self.QA.setQuery(query.lower())

    def setPath(self, path):
        self.paths.append(path)

    def findPathIndex(self):
        tableName = self.QA.findMainTable()

        for i, path in enumerate(self.paths):
            if path.find(tableName):
                pathIndex = i
        
        return pathIndex
    

    def process(self):

        if(self.QA.decodeInput(self.query)):      # Usa o queryAnalyzer para fazer a analise da query
            queryMainTable = self.QA.findMainTable()   # Encontra a tabela principal da query
        else:
            print("Use apenas uma tabela na clausula FROM.")

        # Encontrando a posição da tabela principal da query na lista de tabelas
        for i in range(len(self.tables)):
            if(queryMainTable == self.tables[i].getName()):
                self.indexMainTable = i

        # Tem select
        if(self.QA.hasSelect() and self.indexMainTable != -1):
            self.makeSelect()
        # Tem insert 
        elif(self.QA.getInsert()[0] == True and self.indexMainTable != -1):
            self.makeInsert()
        # Tem delete
        elif(self.QA.getDelete()[0] == True and self.indexMainTable != -1):
            self.makeDelete()
        # Tem update
        elif(self.QA.getUpdate()[0] == True and self.indexMainTable != -1):
            self.makeUpdate()
        else:
            print("Infelizmente ocorreu um erro.")

        if(self.QA.hasOrderBy()):
            self.handleOrderBy()

        for table in self.tables:
            if(table.getName() == "resultTable"):
                table.print()
                self.tables.pop()
            
    def setTable(self, newTable):
        self.tables.append(newTable)
    
    def buildResultTable(self, fields):
        resultTable = Table("resultTable", fields, '')

        # Combina os resultados do WHERE e do ORDER BY
        finalResult = []
        if(self.QA.hasOrderBy() and self.QA.hasWhere()):
            for i in self.orderByFinalResult:
                if i in self.whereResult:
                    finalResult.append(i)
        elif(self.QA.hasWhere()):
            finalResult = self.whereResult
        else:
            finalResult = self.orderByFinalResult

        for field in fields:
            if(not self.QA.hasJoin()):
                resultTable.setFieldRecords(field, self.tables[self.indexMainTable].getFieldRecords(field, finalResult))
            else:
                resultTable.setFieldRecords(field, self.tables[self.indexJoinTable].getFieldRecords(field, finalResult))
        self.tables.append(resultTable)
    
    ##################### Tratando INSERT #####################
    def makeInsert(self):
        fields = self.QA.findFields() # colunas 
        elements = self.QA.findInsertElements() # valor para as colunas

        # Encontra indice do caminho do arquivo
        pathIndex = self.findPathIndex()

        self.file.insert(self.paths[pathIndex], fields, elements)

        # Importa o arquivo novamente apos o delete
        self.setPath(self.paths[pathIndex])
        try:
            # Le arquivo csv e cria uma Table
            table = self.file.read(self.paths[pathIndex],sep=';')
            self.tables.append(table)
            self.setTable(table)
        except:
            print('Erro na leitura, tente novamente.')

    ##################### Tratando DELETE #####################
    def makeDelete(self):

        if(self.QA.hasWhere()):
            self.handleWhere()

        removeList = [x + 1 for x in self.whereResult]

        # Encontra indice do caminho do arquivo
        pathIndex = self.findPathIndex()

        self.file.delete(self.paths[pathIndex], removeList)

        # Importa o arquivo novamente apos o delete
        self.setPath(self.paths[pathIndex])
        try:
            # Le arquivo csv e cria uma Table
            table = self.file.read(self.paths[pathIndex],sep=';')
            self.tables.append(table)
            self.setTable(table)
        except:
            print('Erro na leitura, tente novamente.')
    
    ##################### Tratando UPDATE #####################
    def makeUpdate(self):
        newValues = []
        posRegs = []
        self.file.update(self.paths, posRegs, newValues)

    def makeSelect(self):
        self.whereResult = None
        self.orderByFinalResult = None

        # JOIN
        if(self.QA.hasJoin()):
            self.makeJoin()

        # ORDER BY
        if(self.QA.hasOrderBy()):
            self.handleOrderBy()
        
        # WHERE
        if(self.QA.hasWhere()):
            self.handleWhere()
        
        ############## Tratando asterisco ###############
        if(self.QA.hasAsterisk() and not self.QA.hasJoin()):
            fields = self.tables[self.indexMainTable].getFieldsName()
        elif(self.QA.hasAsterisk()):
            fields = self.tables[self.indexJoinTable].getFieldsName()
        ############## Tratando caso de selecao de campos ###############
        else:
            fields = self.QA.findSelectFields()
        
        self.buildResultTable(fields)

    ############## Tratando WHERE ###############
    def handleWhere(self):
        whereTwoFields = False
        self.whereResult = []
        whereElements = self.QA.findWhereElements()    # Encontra elementos do WHERE
        
        if(len(whereElements) > 3):
            whereTwoFields = True
        
        # Elementos do primeiro campo do WHERE
        whereField1 = whereElements[0]
        whereOperator1 = whereElements[1]
        whereValue1 = whereElements[2]
        
        if(whereField1 in self.QA.queryWords):
            # Pega os registros do primeiro campo usado no WHERE
            whereFieldRecords1 = []
            records1Lower = []
            if(not self.QA.hasJoin()):
                whereFieldRecords1 = self.tables[self.indexMainTable].getFieldRecords(whereField1, None)
            else:
                whereFieldRecords1 = self.tables[self.indexJoinTable].getFieldRecords(whereField1, None)
            
            # Tranforma os registros em minusculo
            for i in range(len(whereFieldRecords1)):
                records1Lower.append(whereFieldRecords1[i].lower())
                
        else:
            print("Campos da clausula WHERE invalidos.")

        if(whereTwoFields):
            # Modificador do WHERE (AND/OR)
            whereModifier = whereElements[3]

            # Elementos do segundo campo do WHERE
            whereField2 = whereElements[4]
            whereOperator2 = whereElements[5]
            whereValue2 = whereElements[6]

            # Pega os registros do segundo campo usado no WHERE
            whereFieldRecords2 = []
            records2Lower = []
            if(not self.QA.hasJoin()):
                whereFieldRecords2 = self.tables[self.indexMainTable].getFieldRecords(whereField2, None)
            else:
                whereFieldRecords2 = self.tables[self.indexJoinTable].getFieldRecords(whereField2, None)

            # Tranforma os registros em minusculo
            for i in range(len(whereFieldRecords2)):
                records2Lower.append(whereFieldRecords2[i].lower())
        
        if(whereFieldRecords1 == None or (whereTwoFields and whereFieldRecords2 == None)):   # Campos do WHERE nao encontrados
            print("Erro na clausula WHERE.")

        match whereOperator1:
            case '=':
                for index, record in enumerate(records1Lower):
                    if(record == whereValue1):
                        self.whereResult.append(index)
            case '>':
                for index, record in enumerate(records1Lower):
                    if(int(record) > int(whereValue1)):
                        self.whereResult.append(index)
            case '<':
                for index, record in enumerate(records1Lower):
                    if(int(record) < int(whereValue1)):
                        self.whereResult.append(index)
            case '>=':
                for index, record in enumerate(records1Lower):
                    if(int(record) >= int(whereValue1)):
                        self.whereResult.append(index)
            case '<=':
                for index, record in enumerate(records1Lower):
                    if(int(record) <= int(whereValue1)):
                        self.whereResult.append(index)
            case '<>':
                for index, record in enumerate(records1Lower):
                    if(record != whereValue1):
                        self.whereResult.append(index)
            case _:
                print("Comparador da clausula WHERE incorreto.")
            
        # Se o WHERE tem dois campos
        if(whereTwoFields):
            match whereOperator2:
                case '=':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((record != whereValue2) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier == 'or'):
                            if((record == whereValue2) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case '>':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((int(record) <= int(whereValue2)) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier == 'or'):
                            if((int(record) > int(whereValue2)) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case '<':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((int(record) >= int(whereValue2)) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier == 'or'):
                            if((int(record) < int(whereValue2)) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case '>=':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((int(record) < int(whereValue2)) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier >= 'or'):
                            if((int(record) == int(whereValue2)) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case '<=':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((int(record) > int(whereValue2)) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier <= 'or'):
                            if((int(record) == int(whereValue2)) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case '<>':
                    for index, record in enumerate(records2Lower):
                        if(whereModifier == 'and'):
                            if((record == whereValue2) and (index in self.whereResult)):
                                self.whereResult.remove(index)
                        elif(whereModifier == 'or'):
                            if((record != whereValue2) and (index not in self.whereResult)):
                                self.whereResult.append(index)
                case _:
                    print("Comparador da clausula WHERE incorreto.")
    
    def handleOrderBy(self):
        OrderByElements = self.QA.findOrderByElements()

        orderByField1 = OrderByElements[0]
        # O default eh ASC
        if(len(OrderByElements) > 1):
            orderByOperator1 = OrderByElements[1]
        else:
            orderByOperator1 = "asc"

        if(len(OrderByElements) > 2):
            orderByField2 = OrderByElements[2]
        
        if(len(OrderByElements) > 3):
            orderByOperator2 = OrderByElements[3]
        else:
            orderByOperator2 = "asc"

        if(orderByField1 in self.QA.queryWords):
            # Pega os registros do campo usado no ORDER BY
            orderByFieldRecords1 = []
            records1Lower = []
            if(not self.QA.hasJoin()):
                orderByFieldRecords1 = self.tables[self.indexMainTable].getFieldRecords(orderByField1, None)
            else:
                orderByFieldRecords1 = self.tables[self.indexJoinTable].getFieldRecords(orderByField1, None)
            
            # Tranforma os registros em minusculo
            for i in range(len(orderByFieldRecords1)):
                records1Lower.append(orderByFieldRecords1[i].lower())
                
        else:
            print("Campos da clausula ORDER BY invalidos.")

        if(len(OrderByElements) > 2):
            if(orderByField2 in self.QA.queryWords):
                # Pega os registros do campo usado no ORDER BY
                orderByFieldRecords2 = []
                records2Lower = []
                if(not self.QA.hasJoin()):
                    orderByFieldRecords2 = self.tables[self.indexMainTable].getFieldRecords(orderByField2, None)
                else:
                    orderByFieldRecords2 = self.tables[self.indexJoinTable].getFieldRecords(orderByField2, None)
                # Tranforma os registros em minusculo
                for i in range(len(orderByFieldRecords2)):
                    records2Lower.append(orderByFieldRecords2[i].lower())
                    
            else:
                print("Campos da clausula ORDER BY invalidos.")

        # Ordenando o primeiro campo do ORDER BY
        sortedRecords1 = self.mergeSort(records1Lower)
        if(orderByOperator1 == "desc"):
            sortedRecords1.reverse()

        # Encontrando a posicao dos elementos ordenados em relacao a tabela original, para o primeiro campo
        orderByResult1 = []
        processedValues = []
        for i in sortedRecords1:
            if i not in processedValues:
                processedValues.append(i)
                for j, elem in enumerate(records1Lower):
                    if(i == elem):
                        orderByResult1.append(j)

        self.orderByFinalResult = []
        if(len(OrderByElements) <= 2):
            self.orderByFinalResult = orderByResult1

        # Tratando caso de dois campos no ORDER BY
        else:
            # Ordenando o segundo campo do ORDER BY
            sortedRecords2 = self.mergeSort(records2Lower)
            if(orderByOperator2 == "desc"):
                sortedRecords2.reverse()

            # Encontrando a posicao dos elementos ordenados em relacao a tabela original, para o segundo campo
            orderByResult2 = []
            processedValues = []
            for i in sortedRecords2:
                if i not in processedValues:
                    processedValues.append(i)
                    for j, elem in enumerate(records2Lower):
                        if(i == elem):
                            orderByResult2.append(j)

            # Encontrando os indices dos elementos repitidos na ordenacao do primeiro campo
            repeatedIndexes = {}
            repIndexes = []
            for index, element in enumerate(orderByFieldRecords1):
                if element in repeatedIndexes:
                    repeatedIndexes[element].append(index)
                else:
                    repeatedIndexes[element] = [index]
            
            for elemento, indices in repeatedIndexes.items():
                if len(indices) > 1:
                    repIndexes.append(indices)
                
            # Juntando resultados da primeira e segunda ordenacao            
            #print(sortedRecords2)
            processedRanges = []
            for i, elem in enumerate(orderByResult1):
                if elem not in processedRanges:
                    processedRanges.append(elem)
                    for element, indexes in repeatedIndexes.items():
                        #print(indexes)
                        if(indexes):
                            if len(indexes) > 1:
                                if elem in indexes:
                                    #print("indx", indexes)
                                    for j, val in enumerate(orderByResult2):
                                        if val in indexes:
                                            self.orderByFinalResult.append(val)
                                            repeatedIndexes[element] = indexes.remove(val)
                                            #print(indexes)
                                            break
                                else:
                                    self.orderByFinalResult.append(elem)
                        else:
                            self.orderByFinalResult.append(elem)
    
    def makeJoin(self):
        nameJoinTable = self.QA.findJoinTable()
        for i, table in enumerate(self.tables):
            if table.getName() == nameJoinTable:
                joinTable = table

        joinField = self.QA.findJoinFields()
        recordsJoinTable = joinTable.getFieldRecords(joinField[0], None)

        mainTable = self.tables[self.indexMainTable]

        recordsMainTable = mainTable.getFieldRecords(joinField[0], None)

        newTableFields = []
        newTableFields.extend(mainTable.getFieldsName())
        newTableFields.extend(joinTable.getFieldsName())
        #newTableFields = list(set(newTableFields))

        newTableRecords = []
        newTableRow = []
        for i, recordMain in enumerate(recordsMainTable):
            for j, recordJoin in enumerate(recordsJoinTable):
                if recordMain == recordJoin:
                    newTableRow = mainTable.getRow(i)
                    newTableRow.extend(joinTable.getRow(j))
                    newTableRecords.append(newTableRow)

        newTable = Table('resultJoinTable', newTableFields, newTableRecords)
        self.setTable(newTable)
        for i, table in enumerate(self.tables):
            if table.getName() == 'resultJoinTable':
                self.indexJoinTable = i

    def mergeSort(self, lst):
        # Verifica se a lista esta vazia ou contem apenas um elemento
        if len(lst) <= 1:
            return lst
        
        # Divide a lista ao meio
        mid = len(lst) // 2
        left_half = lst[:mid]
        right_half = lst[mid:]
        
        # Chamada recursiva para ordenar as duas metades
        left_half = self.mergeSort(left_half)
        right_half = self.mergeSort(right_half)
        
        # Combina as duas metades ordenadas
        return self.merge(left_half, right_half)

    def merge(self, left, right):
        merged = []
        left_index = 0
        right_index = 0
        
        # Combina as duas metades ordenadas em uma nova lista
        while left_index < len(left) and right_index < len(right):
            if left[left_index] < right[right_index]:
                merged.append(left[left_index])
                left_index += 1
            else:
                merged.append(right[right_index])
                right_index += 1
        
        # Adiciona os elementos restantes da metade que não foi completamente percorrida
        merged.extend(left[left_index:])
        merged.extend(right[right_index:])
        
        return merged