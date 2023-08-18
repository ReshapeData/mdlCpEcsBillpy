#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json
from . import salesorder
from . import purchaseorder
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
from . import ecsinterface



def getCode_byOrder(app3, tablename, field, FNumber):
    '''
    通过单据编号获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select distinct {field} from {tablename} where {field}='{FNumber}'"""

    res = app3.select(sql)

    return res


def saleOrderByNumber_sync(FToken,FNumber,FName="赛普集团新账套"):
    '''
    销售订单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_Sales_Order", field="FSALEORDERNO", FNumber=FNumber)

        data = salesorder.salesOrder_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def purchaseOrderByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    采购订单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_pur_poorder", field="FPURORDERNO", FNumber=FNumber)

        data = purchaseorder.purchaseOrder_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def noticeShipmentByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    发货通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

        data = noticeshipment.noticeShipment_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def receiptNoticeByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    收料通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

        data = receiptnotice.receiptNotice_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def saleOutByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    销售出库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

        data = saledelivery.saleOut_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def purchaseStorageByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    采购入库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

        data = purchasestorage.purchaseStorage_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def otherInStockByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    其他入库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

        data = otherinstock.otherInStock_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def otherOutByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    其他出库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

        data = otherout.otherOut_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def salesBillingByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    应收单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_billreceivable", field="FBILLNO", FNumber=FNumber)

        data = salesbilling.salesBilling_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def purchasesBillingByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    应付单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_pur_invoice", field="FBILLNO", FNumber=FNumber)

        data = purchasesbilling.purchasesBilling_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def returnNoticeByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    退货通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

        data = returnnotice.returnNotice_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:
        return False


def returnSaleByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    销售退货单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

        data = returnsales.returnSale_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def returnRequestByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    退料申请单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

        data = returnrequest.returnRequest_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True


    else:

        return False


def returnPurchaseByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    采购退料单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

        data = returnpurchase.returnPurchase_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False


def assemblyDisByNumber_sync(FToken, FNumber, FName="赛普集团新账套"):
    '''
    组装拆卸单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data = getCode_byOrder(app3=app3, tablename="RDS_ECS_ODS_DISASS_DELIVERY", field="FBillNo", FNumber=FNumber)

        data = disassemble.assemblyDis_byOrder(app2=app2, app3=app3, option=option, data=data)

        return True

    else:

        return False



def getDataSource_byOrder(app3, tablename, field, FNumber):
    '''
    通过单据编号获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select * as FQTY from {tablename} where {field}='{FNumber}'"""

    res = app3.select(sql)

    return res


def getDataSource_byDate(app3, tablename, field, FStartDate):
    '''
    通过日期获得数据
    :param FNumber:
    :return:
    '''

    sql = f"""select * from {tablename} where CONVERT(date,{field},20) = '{FStartDate}'"""

    res = app3.select(sql)

    # df=pd.DataFrame(res)

    return res


def saleOrderByNumber_query(token, FNumber):
    '''
    销售订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_Sales_Order", field="FSALEORDERNO",
    #                              FNumber=FNumber)
    #
    # print(data[0])

    sql=f"""select 
    FInterID,
    FSALEORDERNO, 
    FBILLTYPEIDNAME, 
    FSALEDATE, FCUSTCODE,
    FCUSTOMNAME, FSALEORDERENTRYSEQ,
    FPRDNUMBER, FPRDNAME,
    CAST(FQTY as float) as FQTY,
    CAST(FPRICE as float) as FPRICE,
    CAST(FMONEY as float) as FMONEY,
    CAST(FTAXRATE as float) as FTAXRATE,
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT,
    CAST(FTAXPRICE as float) as FTAXPRICE,
    CAST(FALLAMOUNTFOR as float) as FALLAMOUNTFOR
    ,FSALDEPT,FSALGROUP,
    FSALER,FDESCRIPTION,UPDATETIME,FIsfree,
    FIsDO,FPurchaseDate,FCollectionTerms,FUrgency, 
    FSalesType, FUpDateTime, FCurrencyName, FStatus, 
    FMessage, FOccurrenceTime, FRECCONDITIONID, FSETTLETYPEID
    from RDS_ECS_src_Sales_Order where FSALEORDERNO='{FNumber}'"""

    data=app3.select(sql)

    res = pd.DataFrame(data)

    return res



def purchaseOrderByNumber_query(token, FNumber):
    '''
    采购订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_poorder", field="FPURORDERNO",
    #                              FNumber=FNumber)

    sql = f"""select
    FPURORDERNO,
    FBILLTYPENAME,
    FPURCHASEDATE,
    FCUSTOMERNUMBER,
    FSUPPLIERNAME,
    FPOORDERSEQ,
    FPRDNUMBER,
    FPRDNAME,
    CAST(FQTY as float) as FQTY,
    CAST(FPRICE as float) as FPRICE,
    CAST(FAMOUNT as float) as FAMOUNT,
    CAST(FTAXRATE as float) as FTAXRATE,
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT,
    CAST(FTAXPRICE as float) as FTAXPRICE,
    CAST(FORAMOUNTFALL as float) as FORAMOUNTFALL,
    FPURCHASEDEPTID,
    FPURCHASEGROUPID,
    FPURCHASERID,
    FDESCRIPTION,
    FUploadDate,
    FIsDo,
    FDeliveryDate,
    FIsFree,
    FUpDateTime,
    FOrderID,
    FStatus,
    FRECCONDITIONID,
    FMessage,
    FOccurrenceTime,
    FSETTLETYPEID
    from RDS_ECS_src_pur_poorder where FPURORDERNO='{FNumber}'"""

    data=app3.select(sql)
 
    res = pd.DataFrame(data)

    return res



def noticeShipmentByNumber_query(token, FNumber):
    '''
    发货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''
    app3 = RdClient(token=token)

    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
    #                              FNumber=FNumber)

    sql = f"""
    select 
    FInterID, 
    FTRADENO, 
    FDELIVERYNO,
    FBILLTYPE, 
    FDELIVERYSTATUS, 
    FDELIVERDATE,
    FSTOCK,
    FCUSTNUMBER,
    FCUSTOMNAME,
    FORDERTYPE,
    FPRDNUMBER,
    FPRDNAME,
    CAST(FCOSTPRICE as float) as FCOSTPRICE,
    CAST(FPRICE as float) as FPRICE,
    CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
    FLOT,
    FSUMSUPPLIERLOT,
    FPRODUCEDATE,
    FEFFECTIVEDATE,
    FMEASUREUNIT,
    CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
    CAST(FTAXRATE as float) as FTAXRATE,
    FSALER,
    FAUXSALER,
    FCHECKSTATUS,
    UPDATETIME,
    FIsDo,
    FIsfree,
    FDATE,
    FArStatus,
    FOUTID,
    FCurrencyName,
    FMessage,
    FOccurrenceTime
    from RDS_ECS_src_sal_delivery where FDELIVERYNO='{FNumber}'
    """

    data=app3.select(sql)

    res = pd.DataFrame(data)

    return res




def receiptNoticeByNumber_query(token, FNumber):
    '''
    收料通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
    #                              FNumber=FNumber)

    sql = f"""
    select
    FGODOWNNO, 
    FBILLNO, 
    FPOORDERSEQ, 
    FBILLTYPEID, 
    FDOCUMENTSTATUS,
    FSUPPLIERFIELD, 
    FCUSTOMERNUMBER, 
    FSUPPLIERNAME, 
    FSUPPLIERABBR, 
    FSTOCKID, 
    FLIBRARYSIGN, 
    FBUSINESSDATE, 
    FBARCODE, 
    FGOODSID, 
    FPRDNAME, 
    CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
    CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
    CAST(FAMOUNT as float) as FAMOUNT, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    FCHECKSTATUS, 
    FDESCRIPTION, 
    FUPDATETIME, 
    FInstockId, 
    FPRODUCEDATE, 
    FEFFECTIVEDATE, 
    FMessage, 
    FOccurrenceTime, 
    FIsDo
    from RDS_ECS_src_pur_storageacct where FGODOWNNO='{FNumber}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def saleOutByNumber_query(token, FNumber):
    '''
    销售出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
    #                              FNumber=FNumber)

    sql = f"""
        select 
        FInterID, 
        FTRADENO, 
        FDELIVERYNO,
        FBILLTYPE, 
        FDELIVERYSTATUS, 
        FDELIVERDATE,
        FSTOCK,
        FCUSTNUMBER,
        FCUSTOMNAME,
        FORDERTYPE,
        FPRDNUMBER,
        FPRDNAME,
        CAST(FCOSTPRICE as float) as FCOSTPRICE,
        CAST(FPRICE as float) as FPRICE,
        CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
        FLOT,
        FSUMSUPPLIERLOT,
        FPRODUCEDATE,
        FEFFECTIVEDATE,
        FMEASUREUNIT,
        CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
        CAST(FTAXRATE as float) as FTAXRATE,
        FSALER,
        FAUXSALER,
        FCHECKSTATUS,
        UPDATETIME,
        FIsDo,
        FIsfree,
        FDATE,
        FArStatus,
        FOUTID,
        FCurrencyName,
        FMessage,
        FOccurrenceTime
        from RDS_ECS_src_sal_delivery where FDELIVERYNO='{FNumber}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def purchaseStorageByNumber_query(token, FNumber):
    '''
    采购入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
    #                              FNumber=FNumber)

    sql = f"""
        select
        FGODOWNNO, 
        FBILLNO, 
        FPOORDERSEQ, 
        FBILLTYPEID, 
        FDOCUMENTSTATUS,
        FSUPPLIERFIELD, 
        FCUSTOMERNUMBER, 
        FSUPPLIERNAME, 
        FSUPPLIERABBR, 
        FSTOCKID, 
        FLIBRARYSIGN, 
        FBUSINESSDATE, 
        FBARCODE, 
        FGOODSID, 
        FPRDNAME, 
        CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
        CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
        CAST(FAMOUNT as float) as FAMOUNT, 
        CAST(FTAXRATE as float) as FTAXRATE, 
        FLOT, 
        FCHECKSTATUS, 
        FDESCRIPTION, 
        FUPDATETIME, 
        FInstockId, 
        FPRODUCEDATE, 
        FEFFECTIVEDATE, 
        FMessage, 
        FOccurrenceTime, 
        FIsDo
        from RDS_ECS_src_pur_storageacct where FGODOWNNO='{FNumber}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def otherInStockByNumber_query(token, FNumber):
    '''
    其他入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)
    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
    #                              FNumber=FNumber)

    sql = f"""
            select
            FGODOWNNO, 
            FBILLNO, 
            FPOORDERSEQ, 
            FBILLTYPEID, 
            FDOCUMENTSTATUS,
            FSUPPLIERFIELD, 
            FCUSTOMERNUMBER, 
            FSUPPLIERNAME, 
            FSUPPLIERABBR, 
            FSTOCKID, 
            FLIBRARYSIGN, 
            FBUSINESSDATE, 
            FBARCODE, 
            FGOODSID, 
            FPRDNAME, 
            CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
            CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
            CAST(FAMOUNT as float) as FAMOUNT, 
            CAST(FTAXRATE as float) as FTAXRATE, 
            FLOT, 
            FCHECKSTATUS, 
            FDESCRIPTION, 
            FUPDATETIME, 
            FInstockId, 
            FPRODUCEDATE, 
            FEFFECTIVEDATE, 
            FMessage, 
            FOccurrenceTime, 
            FIsDo
            from RDS_ECS_src_pur_storageacct where FGODOWNNO='{FNumber}'
            """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res




def otherOutByNumber_query(token, FNumber):
    '''
    其他出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
    #                                  FNumber=FNumber)

    sql = f"""
            select 
            FInterID, 
            FTRADENO, 
            FDELIVERYNO,
            FBILLTYPE, 
            FDELIVERYSTATUS, 
            FDELIVERDATE,
            FSTOCK,
            FCUSTNUMBER,
            FCUSTOMNAME,
            FORDERTYPE,
            FPRDNUMBER,
            FPRDNAME,
            CAST(FCOSTPRICE as float) as FCOSTPRICE,
            CAST(FPRICE as float) as FPRICE,
            CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
            FLOT,
            FSUMSUPPLIERLOT,
            FPRODUCEDATE,
            FEFFECTIVEDATE,
            FMEASUREUNIT,
            CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
            CAST(FTAXRATE as float) as FTAXRATE,
            FSALER,
            FAUXSALER,
            FCHECKSTATUS,
            UPDATETIME,
            FIsDo,
            FIsfree,
            FDATE,
            FArStatus,
            FOUTID,
            FCurrencyName,
            FMessage,
            FOccurrenceTime
            from RDS_ECS_src_sal_delivery where FDELIVERYNO='{FNumber}'
            """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def salesBillingByNumber_query(token, FNumber):
    '''
    应收单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_billreceivable", field="FBillNo",
    #                              FNumber=FNumber)

    sql = f"""
    select
    FInterID, 
    FCUSTNUMBER, 
    FOUTSTOCKBILLNO, 
    FSALEORDERENTRYSEQ, 
    FBILLTYPEID, 
    FCUSTOMNAME, 
    FBANKBILLNO, 
    FBILLNO, 
    FPrdNumber, 
    FPrdName, 
    FQUANTITY, 
    CAST(FUNITPRICE as float) as FUNITPRICE, 
    CAST(FSUMVALUE as float) as FSUMVALUE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FTRADENO, 
    FNOTETYPE, 
    FISPACKINGBILLNO, 
    FBILLCODE, 
    FINVOICENO, 
    FINVOICEDATE, 
    UPDATETIME, 
    FIsDo, 
    FCurrencyName, 
    FInvoiceid, 
    FLot, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_sal_billreceivable where FBillNo='{FNumber}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res




def purchasesBillingByNumber_query(token, FNumber):
    '''
    应付单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_invoice", field="FBILLNO", FNumber=FNumber)

    sql = f"""
    select
    FPURORDERNO, 
    FGODOWNNO, 
    FBILLTYPEINAME, 
    FINVOICEDATE, 
    FINVOICETYPE, 
    FINVOICENO, 
    FDATE, 
    FCUSTOMERNUMBER, 
    FSUPPLIERNAME, 
    FPOORDERSEQ, 
    FPRDNUMBER, 
    FPRDNAME, 
    CAST(FQTY as float) as FQTY, 
    CAST(FUNITPRICE as float) as FUNITPRICE, 
    CAST(FSUMVALUE as float) as FSUMVALUE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT, 
    CAST(FTAXPRICE as float) as FTAXPRICE, 
    CAST(FAMOUNTALL as float) as FAMOUNTALL, 
    FPURCHASEDEPTNAME, 
    FPURCHASEGROUPNAME, 
    FPURCHASERINAME, 
    FDESCRIPTION, 
    FUPLOADDATE, 
    FISDO, 
    FInvoiceid, 
    FLot, 
    FMessage, 
    FOccurrenceTime, 
    FBILLNO
    from RDS_ECS_src_pur_invoice where FBILLNO='{FNumber}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def returnNoticeByNumber_query(token, FNumber):
    '''
    退货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)
    
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO",
    #                              FNumber=FNumber)

    sql = f"""
    select
    FMRBBILLNO, 
    FTRADENO, 
    FSALEORDERENTRYSEQ, 
    FBILLTYPE, 
    FRETSALESTATE, 
    FPRDRETURNSTATUS, 
    FSTOCK, 
    FCUSTNUMBER, 
    FCUSTOMNAME, 
    FCUSTCODE, 
    FPrdNumber, 
    FPrdName, 
    CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
    CAST(FRETURNQTY as float) as FRETURNQTY, 
    FREQUESTTIME, 
    FBUSINESSDATE, 
    OPTRPTENTRYDATE, 
    CAST(FCOSTPRICE as float) as FCOSTPRICE, 
    FMEASUREUNIT, 
    CAST(FRETAMOUNT as float) as FRETAMOUNT, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    FSALER, 
    FAUXSALER, 
    FSUMSUPPLIERLOT, 
    FPRODUCEDATE, 
    FEFFECTIVEDATE, 
    CAST(FCHECKSTATUS as nvarchar) as FCHECKSTATUS, 
    UPDATETIME, 
    FDELIVERYNO, 
    FIsDo, 
    FIsFree, 
    FReturnTime, 
    FADDID, 
    FCurrencyName, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_sal_returnstock where FMRBBILLNO='{FNumber}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def returnSaleByNumber_query(token, FNumber):
    '''
    销售退货单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)
 
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO",
    #                              FNumber=FNumber)

    sql = f"""
        select
        FMRBBILLNO, 
        FTRADENO, 
        FSALEORDERENTRYSEQ, 
        FBILLTYPE, 
        FRETSALESTATE, 
        FPRDRETURNSTATUS, 
        FSTOCK, 
        FCUSTNUMBER, 
        FCUSTOMNAME, 
        FCUSTCODE, 
        FPrdNumber, 
        FPrdName, 
        CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
        CAST(FRETURNQTY as float) as FRETURNQTY, 
        FREQUESTTIME, 
        FBUSINESSDATE, 
        OPTRPTENTRYDATE, 
        CAST(FCOSTPRICE as float) as FCOSTPRICE, 
        FMEASUREUNIT, 
        CAST(FRETAMOUNT as float) as FRETAMOUNT, 
        CAST(FTAXRATE as float) as FTAXRATE, 
        FLOT, 
        FSALER, 
        FAUXSALER, 
        FSUMSUPPLIERLOT, 
        FPRODUCEDATE, 
        FEFFECTIVEDATE, 
        CAST(FCHECKSTATUS as nvarchar) as FCHECKSTATUS, 
        UPDATETIME, 
        FDELIVERYNO, 
        FIsDo, 
        FIsFree, 
        FReturnTime, 
        FADDID, 
        FCurrencyName, 
        FMessage, 
        FOccurrenceTime
        from RDS_ECS_src_sal_returnstock where FMRBBILLNO='{FNumber}'
        """

    data = app3.select(sql)
      
    res = pd.DataFrame(data)

    return res



def returnRequestByNumber_query(token, FNumber):
    '''
    退料申请单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    sql = f"""
    select
    FMRBBILLNO, 
    FPURORDERNO, 
    FPOORDERSEQ, 
    FBILLTYPEID, 
    FCUSTOMERNUMBER, 
    FSUPPLIERFIELD, 
    FSUPPLIERNAME, 
    FSUPPLIERABBR, 
    FSTOCKID, 
    FGOODSTYPEID, 
    FBARCODE, 
    FGOODSID, 
    FPRDNAME, 
    CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    CAST(FRETQTY as float) as FRETQTY, 
    CAST(FRETAMOUNT as float) as FRETAMOUNT, 
    FDETAILREMARK, 
    FCHECKSTATUS, 
    FUploadDate, 
    FIsDo, 
    FINISHTIME, 
    EFFECTDATE, 
    MANUFACTUREDATE, 
    FDATE, 
    FReturnId, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_pur_return where FMRBBILLNO='{FNumber}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def returnPurchaseByNumber_query(token, FNumber):
    '''
    采购退料单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)
 
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    sql = f"""
        select
        FMRBBILLNO, 
        FPURORDERNO, 
        FPOORDERSEQ, 
        FBILLTYPEID, 
        FCUSTOMERNUMBER, 
        FSUPPLIERFIELD, 
        FSUPPLIERNAME, 
        FSUPPLIERABBR, 
        FSTOCKID, 
        FGOODSTYPEID, 
        FBARCODE, 
        FGOODSID, 
        FPRDNAME, 
        CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
        CAST(FTAXRATE as float) as FTAXRATE, 
        FLOT, 
        CAST(FRETQTY as float) as FRETQTY, 
        CAST(FRETAMOUNT as float) as FRETAMOUNT, 
        FDETAILREMARK, 
        FCHECKSTATUS, 
        FUploadDate, 
        FIsDo, 
        FINISHTIME, 
        EFFECTDATE, 
        MANUFACTUREDATE, 
        FDATE, 
        FReturnId, 
        FMessage, 
        FOccurrenceTime
        from RDS_ECS_src_pur_return where FMRBBILLNO='{FNumber}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res



def assemblyDisByNumber_query(token, FNumber):
    '''
    组装拆卸单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)
  
    # data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_DISASS_DELIVERY", field="FBillNo",
    #                              FNumber=FNumber)

    sql = """
    select 
    FBillNo, 
    Fseq, 
    Fdate, 
    FDeptName, 
    FItemNumber, 
    FItemName, 
    FItemModel, 
    FUnitName, 
    CAST(Fqty as float) as Fqty, 
    FStockName, 
    Flot, 
    Fnote, 
    FInterID, 
    FEFFECTIVEDATE, 
    FPRODUCEDATE, 
    FSUMSUPPLIERLOT, 
    FAFFAIRTYPE, 
    FMessage, 
    FOccurrenceTime, 
    FIsDo
    from RDS_ECS_src_DISASS_DELIVERY where FBillNo='MC202307030001'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def saleOrderByDate_query(token, FStartDate):
    '''
    销售订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_Sales_Order", field="FSALEDATE",
    #                              FStartDate=FStartDate)

    sql = f"""select 
    FInterID,
    FSALEORDERNO,
    FBILLTYPEIDNAME,
    FSALEDATE, FCUSTCODE,
    FCUSTOMNAME, FSALEORDERENTRYSEQ,
    FPRDNUMBER, FPRDNAME,
    CAST(FQTY as float) as FQTY,
    CAST(FPRICE as float) as FPRICE,
    CAST(FMONEY as float) as FMONEY,
    CAST(FTAXRATE as float) as FTAXRATE,
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT,
    CAST(FTAXPRICE as float) as FTAXPRICE,
    CAST(FALLAMOUNTFOR as float) as FALLAMOUNTFOR
    ,FSALDEPT,FSALGROUP,
    FSALER,FDESCRIPTION,UPDATETIME,FIsfree,
    FIsDO,FPurchaseDate,FCollectionTerms,FUrgency,
    FSalesType, FUpDateTime, FCurrencyName, FStatus,
    FMessage, FOccurrenceTime, FRECCONDITIONID, FSETTLETYPEID
    from RDS_ECS_src_Sales_Order 
    where CONVERT(date,FSALEDATE,20) = '{FStartDate}'"""

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def purchaseOrderByDate_query(token, FStartDate):
    '''
    采购订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_poorder", field="FPURCHASEDATE",
    #                              FStartDate=FStartDate)

    sql = f"""select
    FPURORDERNO,
    FBILLTYPENAME,
    FPURCHASEDATE,
    FCUSTOMERNUMBER,
    FSUPPLIERNAME,
    FPOORDERSEQ,
    FPRDNUMBER,
    FPRDNAME,
    CAST(FQTY as float) as FQTY,
    CAST(FPRICE as float) as FPRICE,
    CAST(FAMOUNT as float) as FAMOUNT,
    CAST(FTAXRATE as float) as FTAXRATE,
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT,
    CAST(FTAXPRICE as float) as FTAXPRICE,
    CAST(FORAMOUNTFALL as float) as FORAMOUNTFALL,
    FPURCHASEDEPTID,
    FPURCHASEGROUPID,
    FPURCHASERID,
    FDESCRIPTION,
    FUploadDate,
    FIsDo,
    FDeliveryDate,
    FIsFree,
    FUpDateTime,
    FOrderID,
    FStatus,
    FRECCONDITIONID,
    FMessage,
    FOccurrenceTime,
    FSETTLETYPEID
    from RDS_ECS_src_pur_poorder where CONVERT(date,FPURCHASEDATE,20) = '{FStartDate}'"""

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def noticeShipmentByDate_query(token, FStartDate):
    '''
    发货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''
    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
    select 
    FInterID, 
    FTRADENO, 
    FDELIVERYNO,
    FBILLTYPE, 
    FDELIVERYSTATUS, 
    FDELIVERDATE,
    FSTOCK,
    FCUSTNUMBER,
    FCUSTOMNAME,
    FORDERTYPE,
    FPRDNUMBER,
    FPRDNAME,
    CAST(FCOSTPRICE as float) as FCOSTPRICE,
    CAST(FPRICE as float) as FPRICE,
    CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
    FLOT,
    FSUMSUPPLIERLOT,
    FPRODUCEDATE,
    FEFFECTIVEDATE,
    FMEASUREUNIT,
    CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
    CAST(FTAXRATE as float) as FTAXRATE,
    FSALER,
    FAUXSALER,
    FCHECKSTATUS,
    UPDATETIME,
    FIsDo,
    FIsfree,
    FDATE,
    FArStatus,
    FOUTID,
    FCurrencyName,
    FMessage,
    FOccurrenceTime
    from RDS_ECS_src_sal_delivery where CONVERT(date,FDELIVERDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def receiptNoticeByDate_query(token, FStartDate):
    '''
    收料通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
    select
    FGODOWNNO, 
    FBILLNO, 
    FPOORDERSEQ, 
    FBILLTYPEID, 
    FDOCUMENTSTATUS,
    FSUPPLIERFIELD, 
    FCUSTOMERNUMBER, 
    FSUPPLIERNAME, 
    FSUPPLIERABBR, 
    FSTOCKID, 
    FLIBRARYSIGN, 
    FBUSINESSDATE, 
    FBARCODE, 
    FGOODSID, 
    FPRDNAME, 
    CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
    CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
    CAST(FAMOUNT as float) as FAMOUNT, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    FCHECKSTATUS, 
    FDESCRIPTION, 
    FUPDATETIME, 
    FInstockId, 
    FPRODUCEDATE, 
    FEFFECTIVEDATE, 
    FMessage, 
    FOccurrenceTime, 
    FIsDo
    from RDS_ECS_src_pur_storageacct where CONVERT(date,FBUSINESSDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def saleOutByDate_query(token, FStartDate):
    '''
    销售出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
        select 
        FInterID, 
        FTRADENO, 
        FDELIVERYNO,
        FBILLTYPE, 
        FDELIVERYSTATUS, 
        FDELIVERDATE,
        FSTOCK,
        FCUSTNUMBER,
        FCUSTOMNAME,
        FORDERTYPE,
        FPRDNUMBER,
        FPRDNAME,
        CAST(FCOSTPRICE as float) as FCOSTPRICE,
        CAST(FPRICE as float) as FPRICE,
        CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
        FLOT,
        FSUMSUPPLIERLOT,
        FPRODUCEDATE,
        FEFFECTIVEDATE,
        FMEASUREUNIT,
        CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
        CAST(FTAXRATE as float) as FTAXRATE,
        FSALER,
        FAUXSALER,
        FCHECKSTATUS,
        UPDATETIME,
        FIsDo,
        FIsfree,
        FDATE,
        FArStatus,
        FOUTID,
        FCurrencyName,
        FMessage,
        FOccurrenceTime
        from RDS_ECS_src_sal_delivery where CONVERT(date,FDELIVERDATE,20) = '{FStartDate}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def purchaseStorageByDate_query(token, FStartDate):
    '''
    采购入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
        select
        FGODOWNNO, 
        FBILLNO, 
        FPOORDERSEQ, 
        FBILLTYPEID, 
        FDOCUMENTSTATUS,
        FSUPPLIERFIELD, 
        FCUSTOMERNUMBER, 
        FSUPPLIERNAME, 
        FSUPPLIERABBR, 
        FSTOCKID, 
        FLIBRARYSIGN, 
        FBUSINESSDATE, 
        FBARCODE, 
        FGOODSID, 
        FPRDNAME, 
        CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
        CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
        CAST(FAMOUNT as float) as FAMOUNT, 
        CAST(FTAXRATE as float) as FTAXRATE, 
        FLOT, 
        FCHECKSTATUS, 
        FDESCRIPTION, 
        FUPDATETIME, 
        FInstockId, 
        FPRODUCEDATE, 
        FEFFECTIVEDATE, 
        FMessage, 
        FOccurrenceTime, 
        FIsDo
        from RDS_ECS_src_pur_storageacct where CONVERT(date,FBUSINESSDATE,20) = '{FStartDate}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def otherInStockByDate_query(token, FStartDate):
    '''
    其他入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
            select
            FGODOWNNO, 
            FBILLNO, 
            FPOORDERSEQ, 
            FBILLTYPEID, 
            FDOCUMENTSTATUS,
            FSUPPLIERFIELD, 
            FCUSTOMERNUMBER, 
            FSUPPLIERNAME, 
            FSUPPLIERABBR, 
            FSTOCKID, 
            FLIBRARYSIGN, 
            FBUSINESSDATE, 
            FBARCODE, 
            FGOODSID, 
            FPRDNAME, 
            CAST(FINSTOCKQTY as float) as FINSTOCKQTY, 
            CAST(FPURCHASEPRICE as float) as FPURCHASEPRICE, 
            CAST(FAMOUNT as float) as FAMOUNT, 
            CAST(FTAXRATE as float) as FTAXRATE, 
            FLOT, 
            FCHECKSTATUS, 
            FDESCRIPTION, 
            FUPDATETIME, 
            FInstockId, 
            FPRODUCEDATE, 
            FEFFECTIVEDATE, 
            FMessage, 
            FOccurrenceTime, 
            FIsDo
            from RDS_ECS_src_pur_storageacct where CONVERT(date,FBUSINESSDATE,20) = '{FStartDate}'
            """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def otherOutByDate_query(token, FStartDate):
    '''
    其他出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
            select 
            FInterID, 
            FTRADENO, 
            FDELIVERYNO,
            FBILLTYPE, 
            FDELIVERYSTATUS, 
            FDELIVERDATE,
            FSTOCK,
            FCUSTNUMBER,
            FCUSTOMNAME,
            FORDERTYPE,
            FPRDNUMBER,
            FPRDNAME,
            CAST(FCOSTPRICE as float) as FCOSTPRICE,
            CAST(FPRICE as float) as FPRICE,
            CAST(FNBASEUNITQTY as float) as FNBASEUNITQTY,
            FLOT,
            FSUMSUPPLIERLOT,
            FPRODUCEDATE,
            FEFFECTIVEDATE,
            FMEASUREUNIT,
            CAST(DELIVERYAMOUNT as float) as DELIVERYAMOUNT,
            CAST(FTAXRATE as float) as FTAXRATE,
            FSALER,
            FAUXSALER,
            FCHECKSTATUS,
            UPDATETIME,
            FIsDo,
            FIsfree,
            FDATE,
            FArStatus,
            FOUTID,
            FCurrencyName,
            FMessage,
            FOccurrenceTime
            from RDS_ECS_src_sal_delivery where CONVERT(date,FDELIVERDATE,20) = '{FStartDate}'
            """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def salesBillingByDate_query(token, FStartDate):
    '''
    应收单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_billreceivable", field="FINVOICEDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
    select
    FInterID, 
    FCUSTNUMBER, 
    FOUTSTOCKBILLNO, 
    FSALEORDERENTRYSEQ, 
    FBILLTYPEID, 
    FCUSTOMNAME, 
    FBANKBILLNO, 
    FBILLNO, 
    FPrdNumber, 
    FPrdName, 
    FQUANTITY, 
    CAST(FUNITPRICE as float) as FUNITPRICE, 
    CAST(FSUMVALUE as float) as FSUMVALUE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FTRADENO, 
    FNOTETYPE, 
    FISPACKINGBILLNO, 
    FBILLCODE, 
    FINVOICENO, 
    FINVOICEDATE, 
    UPDATETIME, 
    FIsDo, 
    FCurrencyName, 
    FInvoiceid, 
    FLot, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_sal_billreceivable where CONVERT(date,FINVOICEDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def purchasesBillingByDate_query(token, FStartDate):
    '''
    应付单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_invoice", field="FDate", FStartDate=FStartDate)

    sql = f"""
    select
    FPURORDERNO, 
    FGODOWNNO, 
    FBILLTYPEINAME, 
    FINVOICEDATE, 
    FINVOICETYPE, 
    FINVOICENO, 
    FDATE, 
    FCUSTOMERNUMBER, 
    FSUPPLIERNAME, 
    FPOORDERSEQ, 
    FPRDNUMBER, 
    FPRDNAME, 
    CAST(FQTY as float) as FQTY, 
    CAST(FUNITPRICE as float) as FUNITPRICE, 
    CAST(FSUMVALUE as float) as FSUMVALUE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    CAST(FTAXAMOUNT as float) as FTAXAMOUNT, 
    CAST(FTAXPRICE as float) as FTAXPRICE, 
    CAST(FAMOUNTALL as float) as FAMOUNTALL, 
    FPURCHASEDEPTNAME, 
    FPURCHASEGROUPNAME, 
    FPURCHASERINAME, 
    FDESCRIPTION, 
    FUPLOADDATE, 
    FISDO, 
    FInvoiceid, 
    FLot, 
    FMessage, 
    FOccurrenceTime, 
    FBILLNO
    from RDS_ECS_src_pur_invoice where CONVERT(date,FDate,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def returnNoticeByDate_query(token, FStartDate):
    '''
    退货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="OPTRPTENTRYDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
    select
    FMRBBILLNO, 
    FTRADENO, 
    FSALEORDERENTRYSEQ, 
    FBILLTYPE, 
    FRETSALESTATE, 
    FPRDRETURNSTATUS, 
    FSTOCK, 
    FCUSTNUMBER, 
    FCUSTOMNAME, 
    FCUSTCODE, 
    FPrdNumber, 
    FPrdName, 
    CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
    CAST(FRETURNQTY as float) as FRETURNQTY, 
    FREQUESTTIME, 
    FBUSINESSDATE, 
    OPTRPTENTRYDATE, 
    CAST(FCOSTPRICE as float) as FCOSTPRICE, 
    FMEASUREUNIT, 
    CAST(FRETAMOUNT as float) as FRETAMOUNT, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    FSALER, 
    FAUXSALER, 
    FSUMSUPPLIERLOT, 
    FPRODUCEDATE, 
    FEFFECTIVEDATE, 
    CAST(FCHECKSTATUS as nvarchar) as FCHECKSTATUS, 
    UPDATETIME, 
    FDELIVERYNO, 
    FIsDo, 
    FIsFree, 
    FReturnTime, 
    FADDID, 
    FCurrencyName, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_sal_returnstock where CONVERT(date,OPTRPTENTRYDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def returnSaleByDate_query(token, FStartDate):
    '''
    销售退货单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="OPTRPTENTRYDATE",
    #                              FStartDate=FStartDate)

    sql = f"""
        select
        FMRBBILLNO, 
        FTRADENO, 
        FSALEORDERENTRYSEQ, 
        FBILLTYPE, 
        FRETSALESTATE, 
        FPRDRETURNSTATUS, 
        FSTOCK, 
        FCUSTNUMBER, 
        FCUSTOMNAME, 
        FCUSTCODE, 
        FPrdNumber, 
        FPrdName, 
        CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
        CAST(FRETURNQTY as float) as FRETURNQTY, 
        FREQUESTTIME, 
        FBUSINESSDATE, 
        OPTRPTENTRYDATE, 
        CAST(FCOSTPRICE as float) as FCOSTPRICE, 
        FMEASUREUNIT, 
        CAST(FRETAMOUNT as float) as FRETAMOUNT, 
        CAST(FTAXRATE as float) as FTAXRATE, 
        FLOT, 
        FSALER, 
        FAUXSALER, 
        FSUMSUPPLIERLOT, 
        FPRODUCEDATE, 
        FEFFECTIVEDATE, 
        CAST(FCHECKSTATUS as nvarchar) as FCHECKSTATUS, 
        UPDATETIME, 
        FDELIVERYNO, 
        FIsDo, 
        FIsFree, 
        FReturnTime, 
        FADDID, 
        FCurrencyName, 
        FMessage, 
        FOccurrenceTime
        from RDS_ECS_src_sal_returnstock where CONVERT(date,OPTRPTENTRYDATE,20) = '{FStartDate}'
        """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def returnRequestByDate_query(token, FStartDate):
    '''
    退料申请单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_return", field="FDATE", FStartDate=FStartDate)

    sql = f"""
    select
    FMRBBILLNO, 
    FPURORDERNO, 
    FPOORDERSEQ, 
    FBILLTYPEID, 
    FCUSTOMERNUMBER, 
    FSUPPLIERFIELD, 
    FSUPPLIERNAME, 
    FSUPPLIERABBR, 
    FSTOCKID, 
    FGOODSTYPEID, 
    FBARCODE, 
    FGOODSID, 
    FPRDNAME, 
    CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    CAST(FRETQTY as float) as FRETQTY, 
    CAST(FRETAMOUNT as float) as FRETAMOUNT, 
    FDETAILREMARK, 
    FCHECKSTATUS, 
    FUploadDate, 
    FIsDo, 
    FINISHTIME, 
    EFFECTDATE, 
    MANUFACTUREDATE, 
    FDATE, 
    FReturnId, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_pur_return where CONVERT(date,FDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def returnPurchaseByDate_query(token, FStartDate):
    '''
    采购退料单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_return", field="FDATE", FStartDate=FStartDate)

    sql = f"""
    select
    FMRBBILLNO, 
    FPURORDERNO, 
    FPOORDERSEQ, 
    FBILLTYPEID, 
    FCUSTOMERNUMBER, 
    FSUPPLIERFIELD, 
    FSUPPLIERNAME, 
    FSUPPLIERABBR, 
    FSTOCKID, 
    FGOODSTYPEID, 
    FBARCODE, 
    FGOODSID, 
    FPRDNAME, 
    CAST(FRETSALEPRICE as float) as FRETSALEPRICE, 
    CAST(FTAXRATE as float) as FTAXRATE, 
    FLOT, 
    CAST(FRETQTY as float) as FRETQTY, 
    CAST(FRETAMOUNT as float) as FRETAMOUNT, 
    FDETAILREMARK, 
    FCHECKSTATUS, 
    FUploadDate, 
    FIsDo, 
    FINISHTIME, 
    EFFECTDATE, 
    MANUFACTUREDATE, 
    FDATE, 
    FReturnId, 
    FMessage, 
    FOccurrenceTime
    from RDS_ECS_src_pur_return where CONVERT(date,FDATE,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res


def assemblyDisByDate_query(token, FStartDate):
    '''
    组装拆卸单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    # data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_DISASS_DELIVERY", field="Fdate",
    #                              FStartDate=FStartDate)

    sql = f"""
    select 
    FBillNo, 
    Fseq, 
    Fdate, 
    FDeptName, 
    FItemNumber, 
    FItemName, 
    FItemModel, 
    FUnitName, 
    CAST(Fqty as float) as Fqty, 
    FStockName, 
    Flot, 
    Fnote, 
    FInterID, 
    FEFFECTIVEDATE, 
    FPRODUCEDATE, 
    FSUMSUPPLIERLOT, 
    FAFFAIRTYPE, 
    FMessage, 
    FOccurrenceTime, 
    FIsDo
    from RDS_ECS_src_DISASS_DELIVERY where CONVERT(date,Fdate,20) = '{FStartDate}'
    """

    data = app3.select(sql)

    res = pd.DataFrame(data)

    return res




def ERPData_query(api_sdk, option, FNumber, Formid):
    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])
    model = {
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res = api_sdk.View(Formid, model)

    result = json.loads(res)

    if result["Result"]["ResponseStatus"]["IsSuccess"]:

        return "单据已存在ERP系统"

    else:
        return "单据未存在金蝶系统"
    
    
    
def saleOrderErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    销售订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_SaleOrder")

    return res



def purchaseOrderErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    采购订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="PUR_PurchaseOrder")

    return res





def noticeShipmentErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    发货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_DELIVERYNOTICE")

    return res

    


def receiptNoticeErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    收料通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="PUR_ReceiveBill")

    return res





def saleOutErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    销售出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_OUTSTOCK")

    return res





def purchaseStorageErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    采购入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }


    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="STK_InStock")

    return res





def otherInStockErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    其他入库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }


    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="STK_MISCELLANEOUS")

    return res





def otherOutErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    其他出库单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="STK_MisDelivery")

    return res




def salesBillingErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    应收单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="AR_receivable")

    return res





def purchasesBillingErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    应付单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }


    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="AP_Payable")

    return res





def returnNoticeErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    退货通知单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_RETURNNOTICE")

    return res





def returnSaleErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    销售退货单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_RETURNSTOCK")

    return res





def returnRequestErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    退料申请单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="PUR_MRAPP")

    return res





def returnPurchaseErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    采购退料单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="PUR_MRB")

    return res


def assemblyDisErpDataByFNumber_query(token, FNumber,FName="赛普集团新账套"):
    '''
    组装拆卸单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="STK_AssembledApp")

    return res

def Status_upload(app3, tablename, field, FNumber):
    '''
    修改单据状态
    :param app3:
    :param tablename:
    :param field:
    :param FNumber:
    :return:
    '''

    sql = f"update a set a.{field}=0 from {tablename} a where a.{field}='{FNumber}'"

    app3.update(sql)

    return "单据状态修改已完成"


def DataStatus_update(app3,FTableName,field,FNumber):

    sql=f"""
    update a set a.FIsdo=0 from {FTableName} a where a.{field}='{FNumber}'
    """

    app3.update(sql)


    
def saleOrderStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    销售订单
    :return: 
    '''

    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_Sales_Order", field="FSALEORDERNO", FNumber=FNumber)

    return True
    
         
def purchaseOrderStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '''
    采购订单状态修改
    :param token: 
    :param FNumber: 
    :param FName: 
    :return: 
    '''

    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_poorder", field="FPURORDERNO", FNumber=FNumber)

    return True


def noticeShipmentStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '发货通知单'

    
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    return True
    


def receiptNoticeStatus_upload(token,FNumber,FName="赛普集团新账套"):

    '收料通知单'
    
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    return True
    
    

def saleOutStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '销售出库单'
    
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    return True
    

def purchaseStorageStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '采购入库单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    return True


def otherInStockStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '其他入库单'
    
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    return True


def otherOutStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '其他出库单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    return True


def salesBillingStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '应收单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_billreceivable", field="FBillNo", FNumber=FNumber)

    return True


def purchasesBillingStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '应付单'
    
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_invoice", field="FBILLNO", FNumber=FNumber)

    return True
        
        

def returnNoticeStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '退货通知单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    return True
   
   

def returnSaleStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '销售退货单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    return True
        
        

def returnRequestStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '退料申请单'
   
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    return True


def returnPurchaseStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '采购退料单'
    app3 = RdClient(token=token)

    DataStatus_update(app3=app3, FTableName="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    return True


def assemblyDisStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '''组装拆卸单'''

    app3 = RdClient(token=token)

    Status_upload(app3=app3, tablename="RDS_ECS_ods_DISASS_DELIVERY", field="FBillNo",
                         FNumber=FNumber)
    return True
    

def saleOrderLog_query(token,FNumber):
    '''
    销售订单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='销售订单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def purchaseOrderLog_query(token,FNumber):
    '''
    采购订单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='采购订单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def noticeShipmentLog_query(token,FNumber):
    '''
    发货通知单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='发货通知单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def receiptNoticeLog_query(token,FNumber):
    '''
    收料通知单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='收料通知单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def saleOutLog_query(token,FNumber):
    '''
    销售出库单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='销售出库单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def purchaseStorageLog_query(token,FNumber):
    '''
    采购入库单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='采购入库单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def otherInStockLog_query(token,FNumber):
    '''
    其他入库单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='其他入库单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def otherOutLog_query(token,FNumber):
    '''
    其他出库单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='其他出库单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def salesBillingLog_query(token,FNumber):
    '''
    应收单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='应收单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def purchasesBillingLog_query(token,FNumber):
    '''
    应付单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='应付单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def returnNoticeLog_query(token,FNumber):
    '''
    退货通知单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='退货通知单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def returnSaleLog_query(token,FNumber):
    '''
    销售退货单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='销售退货单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def returnRequestLog_query(token,FNumber):
    '''
    退料申请单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='退料申请单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def returnPurchaseLog_query(token,FNumber):
    '''
    采购退料单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='采购退料单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def assemblyDisLog_query(token,FNumber):
    '''
    组装拆卸单日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='组装拆卸单'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df




def saleOrderByDate_sync(FToken,FDate,FName="赛普集团新账套"):
    '''
    销售订单
    :param FToken:
    :param FDate: 单据日期
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = salesorder.salesOrder(FDate, FDate, app2, app3, option)

        return True

    else:

        return False



def purchaseOrderByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    采购订单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = purchaseorder.purchaseOrder(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def noticeShipmentByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    发货通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = noticeshipment.noticeShipment(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def receiptNoticeByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    收料通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = receiptnotice.receiptNotice(FDate, FDate, app2, app3, option)

        return True

    else:

        return False



def saleOutByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    销售出库单
    :param FToken:
    :param FDate: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = saledelivery.saleOut(FDate, FDate, app2, app3, option)

        return True

    else:

        return False

def purchaseStorageByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    采购入库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = purchasestorage.purchaseStorage(FDate, FDate, app2, app3, option)

        return True

    else:

        return False

def otherInStockByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    其他入库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = otherinstock.otherInStock(FDate, FDate, app2, app3, option)

        return True

    else:

        return False



def otherOutByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    其他出库单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = otherout.otherOut(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def salesBillingByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    应收单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = salesbilling.salesBilling(FDate, FDate, app2, app3, option)

        return True

    else:

        return False

def purchasesBillingByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    应付单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = purchasesbilling.purchasesBilling(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def returnNoticeByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    退货通知单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = returnnotice.returnNotice(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def returnSaleByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    销售退货单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = returnsales.returnSale(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def returnRequestByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    退料申请单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = returnrequest.returnRequest(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def returnPurchaseByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    采购退料单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = returnpurchase.returnPurchase(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


def assemblyDisByDate_sync(FToken, FDate, FName="赛普集团新账套"):
    '''
    组装拆卸单
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=FToken)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = disassemble.assemblyDis(FDate, FDate, app2, app3, option)

        return True

    else:

        return False


