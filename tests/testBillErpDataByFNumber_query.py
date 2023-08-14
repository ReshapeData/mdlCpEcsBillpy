#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # 按查看Erp单据测试用例

    res=newFunc.saleOrderErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="202307030001")

    print(res)

    res=newFunc.purchaseOrderErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="SP202307030002")

    print(res)

    res=newFunc.noticeShipmentErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="D202306200021")

    print(res)

    res = newFunc.receiptNoticeErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307030001")

    print(res)

    res = newFunc.saleOutErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202306200021")

    print(res)

    res = newFunc.purchaseStorageErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307030001")

    print(res)

    res = newFunc.otherInStockErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307240003")

    print(res)

    res = newFunc.otherOutErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202306250022")

    print(res)

    res = newFunc.salesBillingErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="S202307200003")

    print(res)

    res = newFunc.purchasesBillingErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SD202307060001")

    print(res)

    res = newFunc.returnNoticeErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="R202304110001")

    print(res)

    res = newFunc.returnSaleErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="R202304110001")

    print(res)

    res = newFunc.returnRequestErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SR202307250005")

    print(res)

    res = newFunc.returnPurchaseErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SR202307250005")

    print(res)

    res = newFunc.assemblyDisErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="MC202307030001")

    print(res)

