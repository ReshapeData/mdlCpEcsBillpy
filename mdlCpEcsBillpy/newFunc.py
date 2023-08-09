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

    return data



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

    return data


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

    return data



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

    return data



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

    return data



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

    return data


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

    return data



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

    return data


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

    return data


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

    return data


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

    return data


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

    return data


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

    return data


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

    return data


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

    return data



def getDataSource_byOrder(app3, tablename, field, FNumber):
    '''
    通过单据编号获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select * from {tablename} where {field}='{FNumber}'"""

    res = app3.select(sql)

    return res


def getDataSource_byDate(app3, tablename, field, FStartDate):
    '''
    通过日期获得数据
    :param FNumber:
    :return:
    '''

    sql = f"""select * from {tablename} where {field} like '{FStartDate}'"""

    res = app3.select(sql)

    df=pd.DataFrame(res)

    return df


def saleOrderByNumber_query(token, FNumber):
    '''
    销售订单查询
    :param FToken:
    :param FNumber: 单据编号
    :param FName:
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_Sales_Order", field="FSALEORDERNO",
                                 FNumber=FNumber)

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

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_poorder", field="FPURORDERNO",
                                 FNumber=FNumber)
 
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

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
                                 FNumber=FNumber)

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

    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
                                 FNumber=FNumber)

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


    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
                                 FNumber=FNumber)

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

    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
                                 FNumber=FNumber)

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
    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FGODOWNNO",
                                 FNumber=FNumber)

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

    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERYNO",
                                     FNumber=FNumber)

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

    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_billreceivable", field="FBillNo",
                                 FNumber=FNumber)

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

    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_invoice", field="FBILLNO", FNumber=FNumber)

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
    
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO",
                                 FNumber=FNumber)

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
 
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO",
                                 FNumber=FNumber)
      
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

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

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
 
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

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
  
    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_DISASS_DELIVERY", field="FBillNo",
                                 FNumber=FNumber)
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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_Sales_Order", field="FSALEDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_poorder", field="FPURCHASEDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_storageacct", field="FBUSINESSDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_delivery", field="FDELIVERDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_billreceivable", field="FINVOICEDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_invoice", field="FDate", FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="OPTRPTENTRYDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_sal_returnstock", field="OPTRPTENTRYDATE",
                                 FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_return", field="FDATE", FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_pur_return", field="FDATE", FStartDate=FStartDate)

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

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_DISASS_DELIVERY", field="Fdate",
                                 FStartDate=FStartDate)
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


    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="SAL_SaleOrder")

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

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="STK_MISCELLANEOUS")

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


def deleteData(app3,FTableName,field,FNumber):

    sql=f"""
    delete from {FTableName} where {field}='{FNumber}'
    """

    app3.update(sql)


    
def saleOrderStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    销售订单
    :return: 
    '''

    app3 = RdClient(token=token)
    
    sql = f"select * from rds_key_values where FName='{FName}'"
    
    key = app3.select(sql)
    
    app2 = RdClient(token=key[0]["FApp2"])
    
    
    deleteData(app3=app3,FTableName="RDS_ECS_src_Sales_Order",field="FSALEORDERNO",FNumber=FNumber)
    
    deleteData(app3=app3, FTableName="RDS_ECS_ods_Sales_Order", field="FSALEORDERNO", FNumber=FNumber)
    
    df=ecsinterface.writeSRC(FNumber=FNumber,FNumber2=FNumber,app3=app3,viewname="v_sales_order_details",field="FSALEORDERNO")
    
    odernum = list(set(list(df['FSALEORDERNO'])))
    
    df = df.fillna("")
    
    salesorder.insert_SAL_ORDER_Table_byOrder(app2=app2, app3=app3, ordernum=odernum,df=df)

    return "修改成功"
    
         
def purchaseOrderStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '''
    采购订单状态修改
    :param token: 
    :param FNumber: 
    :param FName: 
    :return: 
    '''

    app3 = RdClient(token=token)
    
    sql = f"select * from rds_key_values where FName='{FName}'"
    
    key = app3.select(sql)
    
    app2 = RdClient(token=key[0]["FApp2"])
    
    
    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_poorder", field="FPURORDERNO", FNumber=FNumber)
    
    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_poorder", field="FPURORDERNO", FNumber=FNumber)
    
    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_order",
                               field="FPURORDERNO")
    
    odernum = list(set(list(df['FPURORDERNO'])))
    
    df = df.fillna("")
    
    purchaseorder.insert_procurement_order(app2=app2, app3=app3, odernum=odernum, df=df,api_sdk="",option="")

    return "修改成功"


def noticeShipmentStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '发货通知单'

    
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_delivery",
                               field="FDELIVERYNO")

    df = df.fillna("")

    noticeshipment.insert_sales_delivery(app2,app3, df)

    return "修改成功"
    


def receiptNoticeStatus_upload(token,FNumber,FName="赛普集团新账套"):

    '收料通知单'
    
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_storage",
                               field="FGODOWNNO")

    odernum = list(set(list(df['FGODOWNNO'])))

    df = df.fillna("")

    receiptnotice.insert_procurement_storage(app2, app3, df, odernum)

    return "修改成功"
    
    

def saleOutStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '销售出库单'
    
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_delivery",
                               field="FDELIVERYNO")

    df = df.fillna("")

    noticeshipment.insert_sales_delivery(app2,app3, df)

    return "修改成功"
    

def purchaseStorageStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '采购入库单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_storage",
                               field="FGODOWNNO")

    odernum = list(set(list(df['FGODOWNNO'])))

    df = df.fillna("")

    receiptnotice.insert_procurement_storage(app2, app3, df, odernum)

    return "修改成功"


def otherInStockStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '其他入库单'
    
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_storageacct", field="FGODOWNNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_storage",
                               field="FGODOWNNO")

    odernum = list(set(list(df['FGODOWNNO'])))

    df = df.fillna("")

    receiptnotice.insert_procurement_storage(app2, app3, df, odernum)

    return "修改成功"


def otherOutStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '其他出库单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_delivery", field="FDELIVERYNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_delivery",
                               field="FDELIVERYNO")

    df = df.fillna("")

    noticeshipment.insert_sales_delivery(app2,app3, df)

    return "修改成功"


def salesBillingStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '应收单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_billreceivable", field="FBillNo", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_billreceivable", field="FBillNo", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_invoice",
                               field="FBILLNO")

    df = df.fillna("")

    salesbilling.insert_sales_invoice(app2,app3, df)

    return "修改成功"


def purchasesBillingStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '应付单'
    
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_invoice", field="FBILLNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_invoice", field="FBILLNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_contract",
                               field="FBILLNO")

    df = df.fillna("")

    purchasesbilling.insert_procurement_contract(app2, app3, df)

    return "修改成功"
        
        

def returnNoticeStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '退货通知单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_return",
                               field="FMRBBILLNO")

    df = df.fillna("")

    returnnotice.insert_sales_return(app2, app3, df)

    return "修改成功"
   
   

def returnSaleStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '销售退货单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_sal_returnstock", field="FMRBBILLNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_sales_return",
                               field="FMRBBILLNO")

    df = df.fillna("")

    returnnotice.insert_sales_return(app2, app3, df)

    return "修改成功"
        
        

def returnRequestStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '退料申请单'
   
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_return",
                               field="FMRBBILLNO")

    df = df.fillna("")

    returnrequest.insert_procurement_return(app2, app3, df)

    return "修改成功"


def returnPurchaseStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '采购退料单'
    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    deleteData(app3=app3, FTableName="RDS_ECS_src_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_pur_return", field="FMRBBILLNO", FNumber=FNumber)

    df = ecsinterface.writeSRC(FNumber=FNumber, FNumber2=FNumber, app3=app3, viewname="v_procurement_return",
                               field="FMRBBILLNO")

    df = df.fillna("")

    returnrequest.insert_procurement_return(app2, app3, df)

    return "修改成功"


def assemblyDisStatus_upload(token,FNumber,FName="赛普集团新账套"):
    
    '''组装拆卸单'''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])


    Status_upload(app3=app3, tablename="RDS_ECS_ods_DISASS_DELIVERY", field="FBillNo",
                         FNumber=FNumber)
    
    
    return "修改成功"
    

def log_query(token,FNumber):
    '''
    日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}'"""

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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = salesorder.salesOrder(FDate, FDate, app2, app3, option)

    return res



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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = purchaseorder.purchaseOrder(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = noticeshipment.noticeShipment(FDate, FDate, app2, app3, option)

    return res



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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = receiptnotice.receiptNotice(FDate, FDate, app2, app3, option)

    return res



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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = saledelivery.saleOut(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = purchasestorage.purchaseStorage(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = otherinstock.otherInStock(FDate, FDate, app2, app3, option)

    return res



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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = otherout.otherOut(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = salesbilling.salesBilling(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = purchasesbilling.purchasesBilling(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = returnnotice.returnNotice(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = returnsales.returnSale(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = returnrequest.returnRequest(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = returnpurchase.returnPurchase(FDate, FDate, app2, app3, option)

    return res


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

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = disassemble.assemblyDis(FDate, FDate, app2, app3, option)

    return res


