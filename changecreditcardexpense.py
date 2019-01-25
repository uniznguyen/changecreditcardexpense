import pyodbc
from decimal import *

cn = pyodbc.connect('DSN=QuickBooks Data;',autocommit=True)
cursor = cn.cursor()
encoding = 'utf-8'

sql = """SELECT TxnID, ExpenseLineMemo, ExpenseLineAmount, ExpenseLineCustomerRefFullName, ExpenseLineClassRefFullName, ExpenseLineTxnLineID FROM CreditCardChargeExpenseLine
WHERE TxnDate >= {d'2018-10-01'} AND ExpenseLineAccountRefFullName = 'Field Expenses'
AND ExpenseLineCustomerRefFullName = 'XL Texas Lone Star Auction Lubbock' AND ExpenseLineAmount <> 0"""
values = []

cursor.execute(sql)
for row in cursor.fetchall():
    values.append({'TxnID':row.TxnID,'ExpenseLineMemo':row.ExpenseLineMemo,'ExpenseLineAmount':row.ExpenseLineAmount,
    'ExpenseLineCustomerRefFullName':row.ExpenseLineCustomerRefFullName,
    'ExpenseLineClassRefFullName':row.ExpenseLineClassRefFullName,'ExpenseLineTxnLineID':row.ExpenseLineTxnLineID})

cursor.close()


for i in values:
    
    print (i)

    cursor = cn.cursor()
    try:    
        i['TxnID'] = i['TxnID'].encode(encoding)
        i['ExpenseLineMemo'] = i['ExpenseLineMemo'].encode(encoding)
        i['ItemLineItemRefFullName'] = str("MISC-EQUIP").encode(encoding)
        i['ExpenseLineAmount'] = Decimal(i['ExpenseLineAmount'])
        i['ExpenseLineCustomerRefFullName'] = i['ExpenseLineCustomerRefFullName'].encode(encoding)
        i['ExpenseLineClassRefFullName'] = i['ExpenseLineClassRefFullName'].encode(encoding)
        i['ItemLineQuantity'] = Decimal(1)
        i['BillStatus'] = str("NotBillable").encode(encoding)
        i['ExpenseLineTxnLineID'] = i['ExpenseLineTxnLineID'].encode(encoding)
    except Exception as Error:
        print (Error,i['TxnID'])
        continue

    sql1 = """INSERT INTO CreditCardChargeItemLine (TxnID, ItemLineItemRefFullName, ItemLineDesc, ItemLineQuantity, ItemLineAmount, ItemLineCustomerRefFullName, ItemLineBillableStatus,ItemLineClassRefFullName, FQSaveToCache)
    VALUES (?,?,?,?,?,?,?,?,0)"""

    sql2 = """UPDATE CreditCardChargeExpenseLine
    SET ExpenseLineAmount = 0
    WHERE ExpenseLineTxnLineID = ?"""

    cursor.execute(sql1,i['TxnID'],i['ItemLineItemRefFullName'],i['ExpenseLineMemo'],i['ItemLineQuantity'],i['ExpenseLineAmount'],i['ExpenseLineCustomerRefFullName'],i['BillStatus'],i['ExpenseLineClassRefFullName'])
    cursor.execute(sql2,i['ExpenseLineTxnLineID'])

    cursor.close()


cn.close()




