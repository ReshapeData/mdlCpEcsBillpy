from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # 按单号同步测试用例

    res=newFunc.saleOrderByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="202307030001")

    print(res)

    res=newFunc.purchaseOrderByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="SP202307030002")

    print(res)

    res=newFunc.noticeShipmentByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="D202306200021")

    print(res)

    res=newFunc.receiptNoticeByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307030001")

    print(res)

    res=newFunc.saleOutByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="D202306200021")

    print(res)

    res = newFunc.purchaseStorageByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="PI202307030001")

    print(res)

    res=newFunc.otherInStockByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307240003")

    print(res)

    res = newFunc.otherOutByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202306250022")

    print(res)

    res=newFunc.salesBillingByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="S202307200003")

    print(res)

    res = newFunc.purchasesBillingByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SD202307060001")

    print(res)

    res=newFunc.returnNoticeByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="R202304110001")

    print(res)

    res = newFunc.returnSaleByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="R202304110001")

    print(res)

    res=newFunc.returnRequestByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="SR202307250005")

    print(res)

    res = newFunc.returnPurchaseByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="SR202307250005")

    print(res)

    res=newFunc.assemblyDisByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="MC202307030001")

    print(res)


