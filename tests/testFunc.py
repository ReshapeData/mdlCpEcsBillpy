from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    # SP202307130001

    # res=newFunc.purchaseOrderStatus_upload(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="SP202307310004")
    #
    # print(res)

    # res=newFunc.purchaseOrderByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307310004")
    #
    # print(res)

    # newFunc.receiptNoticeStatus_upload(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307240003")

    # res=newFunc.receiptNoticeByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307240003")
    #
    # print(res)
    #
    # res=newFunc.purchaseStorageByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="PI202307240003")
    #
    # print(res)


    # D202307050004

    # newFunc.noticeShipmentStatus_upload(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", FNumber="D202307310015")

    # res=newFunc.noticeShipmentByNumber_sync(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="D202307310009")
    #
    # print(res)

    # res=newFunc.saleOutByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","D202307310009")
    #
    # print(res)

    # res=newFunc.otherOutByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","D202307260004")
    #
    # print(res)

#     S202304190003

    # newFunc.salesBillingStatus_upload("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","S202304190003")

    # res=newFunc.salesBillingByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","S202304190003")
    #
    # print(res)

    # SD202307310005  SD202307240008

    # res=newFunc.purchasesBillingStatus_upload("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","SD202307310005")

    # R202302100009

    # res=newFunc.returnSaleByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","R202302100009")
    #
    # print(res)
    #
    # res=newFunc.returnNoticeByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","R202302100009")
    #
    # print(res)
    #
    # res.to_excel(r"C:\\Users\\志\\Desktop\\退货单.xlsx")

    # res=newFunc.returnNoticeStatus_upload("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","R202302100009")
    #
    # print(res)


    # res=newFunc.salesBillingByNumber_query("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","S202304190003")
    #
    # print(res)
    #
    # res.to_excel(r"C:\\Users\\志\\Desktop\\应收单异常.xlsx")

    # SD202307310005  SD202307240008

    # res=newFunc.purchasesBillingByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","SD202307310005")
    #
    # print(res)

    # res = newFunc.returnNoticeStatus_upload("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "R202302100009")
    #
    # print(res)
    #
    res=newFunc.returnNoticeByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","R202302100009")

    print(res)

    # res=newFunc.returnSaleByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","R202307190001")
    #
    # print(res)

#     MC202307260001

    # res=newFunc.assemblyDisByNumber_sync("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3","MC202307260001")
    #
    # print(res)

