import pyodbc
from decimal import *


cn = pyodbc.connect('DSN=QuickBooks Data;',autocommit=True)
cursor = cn.cursor()

sql = """SELECT TxnID, ExpenseLineMemo, ExpenseLineAmount, ExpenseLineCustomerRefFullName, ExpenseLineClassRefFullName, ExpenseLineTxnLineID 
FROM CreditCardChargeExpenseLine UNOPTIMIZED
WHERE TxnDate >= {d'2018-01-01'} AND ExpenseLineAccountRefFullName = 'Field Expenses'
AND ExpenseLineCustomerRefFullName IN ('XL Greater Kalamazoo Auto Auction')
AND ExpenseLineAmount <> 0"""
cursor.execute(sql)

values= []
ExpenseLineTxnLineIDs = []
errorlist = []
 
for row in cursor.fetchall():
    values.append({'TxnID':row.TxnID,'ExpenseLineMemo':row.ExpenseLineMemo,'ExpenseLineAmount':row.ExpenseLineAmount,
    'ExpenseLineCustomerRefFullName':row.ExpenseLineCustomerRefFullName,
    'ExpenseLineClassRefFullName':row.ExpenseLineClassRefFullName})
    ExpenseLineTxnLineIDs.append(row.ExpenseLineTxnLineID)

cursor.close()

for i in values:
    try:    
        i['TxnID'] = i['TxnID'].encode()
        i['ExpenseLineMemo'] = i['ExpenseLineMemo'].encode()
        i['ItemLineItemRefFullName'] = str("MISC-EQUIP").encode()
        i['ExpenseLineAmount'] = Decimal(i['ExpenseLineAmount'])
        #i['ExpenseLineCustomerRefFullName'] = i['ExpenseLineCustomerRefFullName'].encode()
        i['ExpenseLineCustomerRefFullName'] = str('XL Texas Lone Star Auction Lubbock:Equipment #1002058').encode()
        i['ExpenseLineClassRefFullName'] = i['ExpenseLineClassRefFullName'].encode()
        i['ItemLineQuantity'] = Decimal(1)
        i['BillStatus'] = str("NotBillable").encode()
    except Exception as Error:
        print (Error,i['TxnID'])
        errorlist.append(i['ExpenseLineAmount'])
        continue

params = list(list(k.values()) for k in values)
print (errorlist)


ExpenseLineTxnLineIDs = tuple(str(i) for i in ExpenseLineTxnLineIDs)


sql1 = """INSERT INTO CreditCardChargeItemLine (TxnID, ItemLineDesc,ItemLineAmount, ItemLineCustomerRefFullName, ItemLineClassRefFullName, ItemLineItemRefFullName,
ItemLineQuantity, ItemLineBillableStatus, FQSaveToCache)
    VALUES (?,?,?,?,?,?,?,?,0)"""

sql2 = f"""UPDATE CreditCardChargeExpenseLine
    SET ExpenseLineAmount = 0
    WHERE ExpenseLineTxnLineID IN {ExpenseLineTxnLineIDs}"""

cursor = cn.cursor()
cn.autocommit = False

cursor.executemany(sql1,params)
cursor.execute(sql2)
cn.commit()

cursor.close()
cn.close()
