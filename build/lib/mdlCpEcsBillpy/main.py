#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrdo.sys import Sys
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from . import salesorder
from . import purchaseorder
import json
from . import receiptnotice
from . import noticeshipment
from . import purchasestorage
from . import saledelivery
from . import salesbilling
from . import purchasesbilling
from . import otherinstock
from . import otherout
from . import returnrequest
from . import returnnotice
from . import returnsales
from . import returnpurchase
from . import disassemble
from . import salesOrderRuturn
from . import salesOrderToNoticeReturn
from pyrda.dbms.rds import RdClient

def getdate():
    app2 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

    mydate = Sys().date()

    print(mydate)

    sql = f"SELECT FStartDate,FEndDate,rdpurchaseorder_FIsDo FROM [dbo].[RDS_ECS_ODS_FDateTime] WHERE rdpurchaseorder_FIsDo=0 and FStartDate<='{mydate}' ORDER  BY FStartDate ASC"

    res = app2.select(sql)

    if res:

        return res

    else:

        return []


def ecsbill_syncBody(startDate, endDate,FName="赛普集团新账套"):

    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    mydate = Sys().date()

    sql=f"select * from rds_key_values where FName='{FName}'"

    key=app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }


    res = salesorder.salesOrder(startDate, endDate,app2,app3,option)
    print(res)

    res = purchaseorder.purchaseOrder(startDate, endDate,app2,app3,option)
    print(res)

    res = noticeshipment.noticeShipment(startDate, endDate,app2,app3,option)
    print(res)

    res = receiptnotice.receiptNotice(startDate, endDate,app2,app3,option)
    print(res)

    res = saledelivery.saleOut(startDate, endDate,app2,app3,option)
    print(res)

    res = purchasestorage.purchaseStorage(startDate, endDate,app2,app3,option)
    print(res)

    res = otherout.otherOut(startDate, endDate,app2,app3,option)
    print(res)

    res = otherinstock.otherInStock(startDate, endDate,app2,app3,option)
    print(res)

    res = salesbilling.salesBilling(startDate, endDate,app2,app3,option)
    print(res)

    res = purchasesbilling.purchasesBilling(startDate, endDate,app2,app3,option)
    print(res)

    res = returnnotice.returnNotice(startDate, endDate,app2,app3,option)
    print(res)

    res = returnrequest.returnRequest(startDate, endDate,app2,app3,option)
    print(res)

    res = returnsales.returnSale(startDate, endDate,app2,app3,option)
    print(res)

    res = returnpurchase.returnPurchase(startDate, endDate,app2,app3,option)
    print(res)

    res=disassemble.assemblyDis(startDate,endDate,app2,app3,option)
    print(res)

    if Sys().now() > str(mydate + " 20:59:59"):

        sql1 = f"update a set a.rdpurchaseorder_FIsDo=1 from RDS_ECS_ODS_FDateTime a where a.FStartDate='{startDate}'"

        app3.update(sql1)

        print("同步完成")

def ecsbill_sync():

    date = getdate()

    msg="This is one"

    for i in date:

        ecsbill_syncBody(str(i['FStartDate']), str(i['FEndDate']))

    return msg


def ecsbill_sync2(startdate,enddate):

    msg = "This is two"

    ecsbill_syncBody(startdate,enddate)

    return msg


def test(option):
    api_sdk = K3CloudApiSdk()

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    model = {
        "CreateOrgId": 0,
        "Numbers": "SR202307050091",
        "Ids": "",
        "InterationFlags": "",
        "IgnoreInterationFlag": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": ""
    }

    res = json.loads(api_sdk.UnAudit("PUR_MRB", model))

    print(res)












