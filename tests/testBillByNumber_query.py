#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # 按单号查询测试用例

    # res=newFunc.saleOrderByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","202307030001")
    #
    # print(res)

    # res=newFunc.purchaseOrderByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","SP202307030002")
    #
    # print(res)
    #
    # res=newFunc.noticeShipmentByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","D202306200021")
    #
    # print(res)
    #
    # res = newFunc.receiptNoticeByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "PI202307030001")
    #
    # print(res)
    #
    # res = newFunc.saleOutByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "D202306200021")
    #
    # print(res)
    #
    # res = newFunc.purchaseStorageByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "PI202307030001")
    #
    # print(res)
    #
    # res = newFunc.otherInStockByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "PI202307240003")
    #
    # print(res)
    #
    # res = newFunc.otherOutByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "D202306250022")
    #
    # print(res)
    #
    # res = newFunc.salesBillingByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "S202307200003")
    #
    # print(res)
    #
    # res = newFunc.purchasesBillingByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "SD202307060001")
    #
    # print(res)
    #
    # res = newFunc.returnNoticeByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "R202304110001")
    #
    # print(res)
    #
    # res = newFunc.returnSaleByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "R202304110001")
    #
    # print(res)
    #
    # res = newFunc.returnRequestByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "SR202307250005")
    #
    # print(res)
    #
    # res = newFunc.returnPurchaseByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "SR202307250005")
    #
    # print(res)
    #
    res = newFunc.assemblyDisByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "MC202307030001")

    print(res)
