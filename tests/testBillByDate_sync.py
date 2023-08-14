#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # 按日期同步测试用例

    res=newFunc.saleOrderByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-05")

    print(res)

    res=newFunc.purchaseOrderByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-05")

    print(res)

    res=newFunc.noticeShipmentByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-05")

    print(res)

    res = newFunc.receiptNoticeByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.saleOutByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.purchaseStorageByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.otherInStockByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.otherOutByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.salesBillingByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.purchasesBillingByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.returnNoticeByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.returnSaleByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.returnRequestByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.returnPurchaseByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

    res = newFunc.assemblyDisByDate_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FDate="2023-07-05")

    print(res)

