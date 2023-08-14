#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    res=newFunc.saleOrderLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="202307030001")

    print(res)

    res=newFunc.purchaseOrderLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="SP202307030002")

    print(res)

    res=newFunc.noticeShipmentLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="D202306200021")

    print(res)

    res = newFunc.receiptNoticeLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307030001")

    print(res)

    res = newFunc.saleOutLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202306200021")

    print(res)

    res = newFunc.purchaseStorageLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307030001")

    print(res)

    res = newFunc.otherInStockLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307240003")

    print(res)

    res = newFunc.otherOutLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202306250022")

    print(res)

    res = newFunc.salesBillingLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="S202307200003")

    print(res)

    res = newFunc.purchasesBillingLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SD202307060001")

    print(res)

    res = newFunc.returnNoticeLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="R202304110001")

    print(res)

    res = newFunc.returnSaleLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="R202304110001")

    print(res)

    res = newFunc.returnRequestLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SR202307250005")

    print(res)

    res = newFunc.returnPurchaseLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SR202307250005")

    print(res)

    res = newFunc.assemblyDisLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="MC202307030001")

    print(res)
