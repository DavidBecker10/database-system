# Analisador de query

from table import Table

class QueryAnalyzer():
    
    def __init__(self):
        self.query = str
        self.queryWords = []

    def setQuery(self, query):
        self.query = query.lower()
    
    # transforma as palavras da busca em uma lista e verifica se ha apenas uma tabela principal
    def decodeInput(self, input):
        self.queryWords = input.split()
        
        mainTable = self.findMainTable()
        OneTable = True
        
        if ',' in mainTable:
            OneTable = False
        for word in self.queryWords:    # Retirando as virgulas
            if ',' in word:
                self.queryWords = [sub.replace(word, word[:-1]) for sub in self.queryWords]
        
        return OneTable
    
    def findJoinTable(self):
        joinTable = self.queryWords[self.getJoin()[1]+1]
        return joinTable
    
    def getJoin(self):
        wordJoin = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "join":
                wordJoin[0] = True
                wordJoin[1] = pos
        return wordJoin

    def getSelect(self):
        wordSelect = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "select":
                wordSelect[0] = True
                wordSelect[1] = pos
        return wordSelect
    
    def getFrom(self):
        wordFrom = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "from":
                wordFrom[0] = True
                wordFrom[1] = pos
        return wordFrom
    
    def getInsert(self):
        wordInsert = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "insert":
                wordInsert[0] = True   
                wordInsert[1] = pos
                break
        return wordInsert
    
    def getValuesInsert(self):
        wordValuesInsert = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "values":
                wordValuesInsert[0] = True
                wordValuesInsert[1] = pos
        return wordValuesInsert
    
    def getDelete(self):
        wordDelete = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "delete":
                wordDelete[0] = True   
                wordDelete[1] = pos
                break
        return wordDelete
    
    def getUpdate(self):
        wordUpdate = [False, -1] # se existe e a posição, -1 se não existe
        for pos, word in enumerate(self.queryWords):
            if word == "update":
                wordUpdate[0] = True   
                wordUpdate[1] = pos
                break
        return wordUpdate
    
    def getWhere(self):
        wordWhere = [False, -1] # se existe e a posição, -1 se não existe
        
        if(self.hasWhere()):
            for pos, word in enumerate(self.queryWords):
                if word == "where":
                    wordWhere[0] = True
                    wordWhere[1] = pos
        
        return wordWhere
    
    def getOrderBy(self):
        wordOrderBy = [False, -1] # se existe e a posição, -1 se não existe
        twoFields = False
        if(self.hasOrderBy()):
            if(self.query.find(',', self.query.find("order by"), len(self.query))):  # Se tem ',' na clausula order by
                twoFields = True
            for pos, word in enumerate(self.queryWords):
                if word == "order" and (self.queryWords[pos+1] == "by"):
                    wordOrderBy[0] = True
                    wordOrderBy[1] = pos
        
        return wordOrderBy

    def findMainTable(self):
        wordFrom = self.getFrom()
        wordInsert = self.getInsert()
        wordDelete = self.getDelete()
        wordUpdate = self.getUpdate()
        wordMainTable = [False, -1]
        if wordFrom[0]: # se tem o from então é uma determinada pos q ta a tabela
            wordMainTable[0] = True
            wordMainTable[1] = wordFrom[1]+1
        elif wordInsert[0]: # se tem o insert 
            wordMainTable[0] = True
            wordMainTable[1] = wordInsert[1]+2 # tem o INTO no meio, por isso soma 2
        elif wordDelete[0]: # se tem o delete
            wordMainTable[0] = True
            wordMainTable[1] = wordDelete[1]+2
        elif wordUpdate[0]: # se tem o update
            wordMainTable[0] = True
            wordMainTable[1] = wordUpdate[1]+1
        mainTable = self.queryWords[wordMainTable[1]]
        return mainTable
    
    def findFields(self):
        fields = []
        hasInsert = self.getInsert()
        hasSelect = self.getSelect()
        if hasSelect[0] == True: # select
            fields = self.findSelectFields()
        elif hasInsert[0] == True: # insert
            fields = self.findInsertFields()
        return fields
    
    def findSelectFields(self):
        fields = []
        if(self.hasSelect()):
            wordFrom = self.getFrom()
            for i in range(1, wordFrom[1]):       # Pega todas a palavras entre o 'select' e o 'from'
                fields.append(self.queryWords[i])
        
        return fields
    
    def findInsertFields(self): # campos selecionados do select
        fields = []
        wordValue = self.getValuesInsert()
        wordInsert = self.getInsert()
        pos = 3 # pega todas as palavras entre a tabela e o 'values'
        while pos > (wordInsert[1] + 2) and pos < wordValue[1]:
            fields.append(self.queryWords[pos])
            pos = pos + 1
        return fields
    
    def findJoinFields(self):
        joinFields = []
        if(self.hasJoin()):
            wordJoin = self.getJoin()
            if(self.hasUsing()):
                joinFields.append(self.queryWords[wordJoin[1]+3])
            #else (on)
        return joinFields
    
    def findInsertElements(self): # campos selecionados do insert
        insertElements = []
        wordValue= self.getValuesInsert() #ok
        pos = wordValue[1]+1 # pega todas as palavras que vem depois de 'values'
        while pos <= len(self.queryWords)-1:
            insertElements.append(self.queryWords[pos])
            pos = pos + 1
        return insertElements
    
    # fazer uma função pra encontrar os campos do update
    
    # Encontra os elementos da clausula WHERE, em todos os casos tratados tem no minimo
    # tres campos (WHERE field 'comparacao' valor) e no maximo 6
    def findWhereElements(self):
        whereElements = []
        if(self.hasWhere()):
            wordWhere = self.getWhere()
            for i in range(wordWhere[1]+1, len(self.queryWords)):    # Pega todos os elementos após o 'where'
                if(self.queryWords[i] != "order"):
                    whereElements.append(self.queryWords[i])
                else:
                    break

        return whereElements
    
    def findOrderByElements(self):
        OrderByElements = []
        if(self.hasOrderBy()):
            wordOrderBy = self.getOrderBy()
            for i in range(wordOrderBy[1]+2, len(self.queryWords)):    # Pega todos os elementos após o 'order by'
                if (i == wordOrderBy[1]+3) and (self.queryWords[i] != "asc" and self.queryWords[i] != "desc"):
                    OrderByElements.append('')
                    OrderByElements.append(self.queryWords[i])
                else:
                    OrderByElements.append(self.queryWords[i])
        return OrderByElements
    
    def hasAsterisk(self):
        if(self.query.find("*") != -1):
            return True
        return False

    def hasWhere(self):
        if(self.query.find("where") != -1):
            return True
        return False
    
    def hasOrderBy(self):
        if(self.query.find("order by") != -1):
            return True
        return False
    
    def hasSelect(self):
        if(self.query.find("select") != -1):
            return True
        return False
    
    def hasJoin(self):
        if(self.query.find("join") != -1):
            return True
        return False
    
    def hasUsing(self):
        if(self.query.find("using") != -1):
            return True
        return False
    
    def hasOn(self):
        if(self.query.find("on") != -1):
            return True
        return False