import gspread , os , calendar , time , sys
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery

credentials_path = 'G:\Meu Drive\CODES\GLOBAL-config\global\sa.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()
newMonth = []
metasTable = "sacred-drive-353312.metas.metas"

def floatChecker(value):
    try:
        float(value)
        return True
    except ValueError:
        print("O valor {} não é float. Arrume a formatação e tente novamente. Exemplo de float: 12000.50 (Doze mil e cinquenta)".format(value))
        time.sleep(3)
        sys.exit(0)


def monthChecker(year , month , store , monthMaxDay):
    payload = "data BETWEEN '{y}-{m}-01T00:00:00.00' AND '{y}-{m}-{d}T23:59:59.999' and ecommerceName = '{s}'".format(y = year , m = month , d = monthMaxDay , s = store)
    result = read("*" , metasTable , payload)
    for row in result:
        return row
    return

def storeChecker(storeList):
    storeNameList = []
    linxQuery = read("DISTINCT store" , "sacred-drive-353312.config_linx.storesConfig" , "TRUE")
    vtexQuery = read("DISTINCT store" , "sacred-drive-353312.config_vtex.storesConfig" , "TRUE")
    otherStores = read("DISTINCT store" , "sacred-drive-353312.metas.outras_lojas" , "TRUE")
    for row in linxQuery:
        storeNameList.append(row.store)
    for row in vtexQuery:
        storeNameList.append(row.store)
    for row in otherStores:
        storeNameList.append(row.store)
    test = [elemento for elemento in storeList if elemento in storeNameList]
    if len(storeList) == len(test):
        return True
    else:
        print("Loja não encontrada, verifique a grafia, feche o programa e tente novamente.")
        print(" ")
        print("Lista de nomes não encontrados: ")
        for store in [elemento for elemento in storeList if elemento not in storeNameList]:
            print(store)
        print(" ")
        print("Gostaria de ler os nomes disponíveis?")
        userInput = str(input('Digite "s" ou "n" e aperte enter: '))
        if userInput.lower() == "s" or userInput.lower() == "easter egg":
            if userInput.lower() == "s":
                for row in storeNameList:
                    print(row)
                time.sleep(10)
            else:
                print("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
                time.sleep(3)
        sys.exit(0)
    
#checa qual a última linha preenchida
def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return str(len(str_list))

def create(values):

    query_job = client.query("""
        INSERT INTO `{}` (data , ecommerceName , meta)
        VALUES {}
        """.format(metasTable, values))  
    result = query_job.result()
    return result

def read(select , table , condition):
    query_job = client.query("""
        SELECT {}
        FROM {}
        WHERE {}
        """.format(select , table , condition))  
    result = query_job.result()
    return result

def update(update , condition):
    query_job = client.query("""
        UPDATE {}
        SET {}
        WHERE {};
        """.format(metasTable , update , condition)) 
    query_job.result()

def insertGoals(date , store , goal):
    values = ""
    dayCounter = 1
    while dayCounter <= monthMaxDay:
            values = values + "('{y}-{m}-{d}T00:00:00.000' , '{s}' , {me})".format(y = date[0:4] , m = date[5:7] , d = str(dayCounter).zfill(2) , s = store , me = goal)
            if dayCounter != monthMaxDay:
                values = values + ","
            dayCounter = dayCounter + 1
    create(values)

    return


#nesse trecho é feita a leitura da planilha 
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name("G:\\Meu Drive\\CODES\\GOOGLESHEET-READER\\sacred-drive-353312-aef176c6bf02.json" , scopes=scopes)

file = gspread.authorize(creds)
workbook = file.open("Inserir metas")
sheet = workbook.sheet1
sheetRange = next_available_row(sheet)
sheetRange = "A2:C" + sheetRange

metas = []
storeList = []
store = []


print("-----------------------------*~*-----------------------------")

#cria a lista de metas e de lojas
counter = 1
for cell in sheet.range(sheetRange):
    metas.append(cell.value)
    store.append(cell.value)
    if counter == 3:
         counter = 1
         floatChecker(store[2])
         storeList.append(store[1])
         store.clear()
    else:
        counter = counter + 1 

#checa a existência das lojas 
storeChecker(list(set(storeList)))

counter = 0
creationList = []
date = -3
store = -2
meta = -1

while len(metas) // 3 != counter: # 4x
    date = date + 3
    store = store + 3
    meta = meta + 3
    dayCounter = 1
    monthMaxDay = calendar.monthrange(int(metas[date][0:4]),int(metas[date][5:7]))[1]
    if monthChecker(metas[date][0:4] , metas[date][5:7] , metas[store] , monthMaxDay) is None:
        insertGoals(metas[date] , metas[store] , float(metas[meta]) / monthMaxDay)
        print(" INSERIDO  -- Loja: {}  |  Data: {}  |  Valor: {} ".format(f'{metas[store]:<18}' , metas[date][0:7] , float(metas[meta]) / monthMaxDay))
    else:
        update("meta = {}".format(float(metas[meta]) / monthMaxDay) , "data BETWEEN '{y}-{m}-01T00:00:00.00' AND '{y}-{m}-{d}T23:59:59.999' and ecommerceName = '{s}'".format(y = metas[date][0:4] , m = metas[date][5:7] , d = monthMaxDay , s = metas[store]))
        print("ATUALIZADO -- Loja: {}  |  Data: {}  |  Valor: {} ".format(f'{metas[store]:<18}' , metas[date][0:7] , float(metas[meta]) / monthMaxDay))


    counter = counter + 1
