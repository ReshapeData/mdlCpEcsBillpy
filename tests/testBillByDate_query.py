#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # 按日期查询测试用例

    res=newFunc.saleOrderByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FStartDate="2023-07-05")

    print(res)
    #
    res=newFunc.purchaseOrderByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FStartDate="2023-07-06")

    print(res)

    res=newFunc.noticeShipmentByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FStartDate="2023-07-21")

    print(res)

    res = newFunc.receiptNoticeByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.saleOutByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.purchaseStorageByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.otherInStockByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.otherOutByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.salesBillingByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

    res = newFunc.purchasesBillingByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-31")

    print(res)

    res = newFunc.returnNoticeByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-30")

    print(res)

    res = newFunc.returnSaleByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-30")

    print(res)

    res = newFunc.returnRequestByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-25")

    print(res)

    res = newFunc.returnPurchaseByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-25")

    print(res)

    res = newFunc.assemblyDisByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FStartDate="2023-07-05")

    print(res)

