#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
import hashlib
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk

import pandas as pd


def encryption(pageNum,pageSize,queryList,tableName):
    '''
    ECS的token加密
    :param pageNum:
    :param pageSize:
    :param queryList:
    :param tableName:
    :return:
    '''

    m = hashlib.md5()

    token=f'accessId=skyx@prod&accessKey=skyx@0512@1024@prod&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

    # token = f'accessId=skyx&accessKey=skyx@0512@1024&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

    m.update(token.encode())

    md5 = m.hexdigest()

    return md5


def ECS_post_info2(url,pageNum,pageSize,qw,qw2,tableName,updateTime,updateTime2,key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''

    try:

        queryList='[{"qw":'+f'"{qw}"'+',"value":'+f'"{updateTime}"'+',"key":'+f'"{key}"'+'},{"qw":'+f'"{qw2}"'+',"value":'+f'"{updateTime2}"'+',"key":'+f'"{key}"'+'}]'

        # 查询条件
        queryList1=[{"qw":qw,"value":updateTime,"key":key},{"qw":qw2,"value":updateTime2,"key":key}]

        # 查询的表名
        tableName=tableName

        data ={
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()
        #
        # print(info)

        col=["FSALEORDERNO","FBILLTYPEIDNAME","FSALEDATE","FCUSTCODE","FCUSTOMNAME","FSALEORDERENTRYSEQ","FPRDNUMBER","FPRDNAME","FQTY","FPRICE","FMONEY","FTAXRATE","FTAXAMOUNT","FTAXPRICE","FAMOUNT","FSALDEPTID","FSALGROUPID","FSALERID","FDESCRIPTION","FCURRENCYID","UPDATETIME","FStatus","FORDERTYPE","FRECCONDITIONID","FSETTLETYPEID"]

        df = pd.DataFrame(info['data']['list'],columns=col)

        # df = pd.DataFrame(info['data']['list'])

        return df

    except Exception as e:

        return pd.DataFrame()


def viewPage(url,pageNum,pageSize,qw,qw2,tableName,updateTime,updateTime2,key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''

    try:

        queryList='[{"qw":'+f'"{qw}"'+',"value":'+f'"{updateTime}"'+',"key":'+f'"{key}"'+'},{"qw":'+f'"{qw2}"'+',"value":'+f'"{updateTime2}"'+',"key":'+f'"{key}"'+'}]'

        # 查询条件
        queryList1=[{"qw":qw,"value":updateTime,"key":key},{"qw":qw2,"value":updateTime2,"key":key}]

        # 查询的表名
        tableName=tableName

        data ={
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()

        return info['data']['pages']

    except Exception as e:

        return []

def json_model(app2,model_data):
    '''
    物料单元model
    :param model_data: 物料信息
    :return:
    '''



    if code_conversion(app2,"rds_vw_material","F_SZSP_SKUNUMBER",model_data['FPrdNumber'])!="" or model_data['FPrdNumber']=='1':

        model={
                "FRowType": "Standard" if model_data['FPrdNumber']!='1' else "Service",
                "FMaterialId": {
                    "FNumber": "7.1.000001" if model_data['FPrdNumber']=='1' else str(code_conversion(app2,"rds_vw_material","F_SZSP_SKUNUMBER",model_data['FPrdNumber']))
                },
                "FQty": str(model_data['FRETURNQTY']),
                # "FPrice": str(model_data['FPRICE']),
                "FTaxPrice": str(model_data['FRETSALEPRICE']),
                "FIsFree": True if float(model_data['FIsFree'])== 1 else False,
                "FEntryTaxRate": float(model_data['FTAXRATE'])*100,
                "FExpPeriod": 1095,
                "FExpUnit": "D",
                "FDeliveryDate": str(model_data['FReturnTime']),
                "FStockOrgId": {
                    "FNumber": "104"
                },
                "FSettleOrgIds": {
                    "FNumber": "104"
                },
                "FSupplyOrgId": {
                    "FNumber": "104"
                },
                "FOwnerTypeId": "BD_OwnerOrg",
                "FOwnerId": {
                    "FNumber": "104"
                },
                # "FEntryNote": "销售出库单",
                "FReserveType": "1",
                "FPriceBaseQty": str(model_data['FRETURNQTY']),
                "FStockQty": str(model_data['FRETURNQTY']),
                "FStockBaseQty": str(model_data['FRETURNQTY']),
                "FOUTLMTUNIT": "SAL",
                "FISMRP": False,
                "F_SZSP_FSPC1": False,
                "FAllAmountExceptDisCount": str(model_data['FRETAMOUNT']),
                # "FOrderEntryPlan": [
                #     {
                #         "FPlanQty": str(model_data['FRETURNQTY'])
                #     }
                # ],
                "FBaseCanReturnQty":str(model_data['FRETURNQTY']),
                "FStockBaseCanReturnQty":str(model_data['FRETURNQTY'])
            }

        return model

    else:

        return False


def data_splicing(app2,data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data:
    :return:
    '''

    try:
        list=[]

        for i in data:

            result=json_model(app2,i)

            if result:

                list.append(result)

            else:

                return []

        return list

    except Exception as e:

        return []

def ERP_Save(api_sdk,data,option,app2,app3):

    '''
    调用ERP保存接口
    :param api_sdk: 调用ERP对象
    :param data:  要插入的数据
    :param option: ERP密钥
    :param app2: 数据库执行对象
    :return:
    '''

    erro_list=[]
    sucess_num=0
    erro_num=0

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        try:

            if check_order_exists(api_sdk,i[0]['FDELIVERYNO'])!=True:

                    model = {
                        "Model": {
                            "FID": 0,
                            "FBillTypeID": {
                                "FNUMBER": "XSDD05_SYS"

                            },
                            "FBillNo": str(i[0]['FDELIVERYNO']),
                            "FDate": str(i[0]['OPTRPTENTRYDATE']),
                            "FSaleOrgId": {
                                "FNumber": "104"
                            },
                            "FCustId": {
                                "FNumber": "C003142"if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FReceiveId": {
                                "FNumber": "C003142"if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FSaleDeptId": {
                                "FNumber": code_conversion(app2,"rds_vw_department","FNAME","销售部")
                            },
                            "FSaleGroupId": {
                                "FNumber": "SKYX01"
                            },
                            "FSalerId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_salesman","FNAME",i[0]['FSALER'],'104')
                            },
                            "FSettleId": {
                                "FNumber": "C003142"if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FChargeId": {
                                "FNumber": "C003142"if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FISINIT": False,
                            "FNote": "销售出库单",
                            "FIsMobile": False,
                            "FIsUseOEMBomPush": False,
                            "FIsUseDrpSalePOPush": False,
                            "F_SZSP_XSLX": {
                                "FNumber": "1"
                            },
                            "F_SZSP_JJCD": {
                                "FNumber": "YB"
                            },
                            "FSaleOrderFinance": {
                                "FSettleCurrId": {
                                    "FNumber": "PRE001" if i[0]['FCurrencyName']=='' else code_conversion(app2,"rds_vw_currency","FNAME",i[0]['FCurrencyName'])
                                },
                                "FRecConditionId": {
                                    "FNumber":"SKTJ05_SP"
                                },
                                "FIsPriceExcludeTax": True,
                                "FIsIncludedTax": True,
                                "FSettleModeId": {
                                    "FNumber": "JSFS04_SYS"
                                },
                                "FExchangeTypeId": {
                                    "FNumber": "HLTX01_SYS"
                                },
                                "FOverOrgTransDirect": False
                            },
                            "FSaleOrderEntry": data_splicing(app2,i),
                            "FSaleOrderPlan": [
                                {
                                    "FNeedRecAdvance": False,
                                    "FRecAdvanceRate": 100.0,
                                    "FIsOutStockByRecamount": False
                                }
                            ]
                        }
                    }

                    save_result=api_sdk.Save("SAL_SaleOrder", model)

                    res=json.loads(save_result)

                    if res['Result']['ResponseStatus']['IsSuccess']:

                        submit_result=ERP_Submit(api_sdk,i[0]['FDELIVERYNO'])

                        if submit_result:

                            sudit_result=ERP_Audit(api_sdk,i[0]['FDELIVERYNO'])

                            if sudit_result:

                                changeStatus(app3,i[0]['FMRBBILLNO'],"1")

                                sucess_num=sucess_num+1

                                insertLog(app3, "销售订单", i[0]['FMRBBILLNO'],"数据同步成功","1")


                            else:
                                pass

                        else:

                            pass


                    else:
                        insertLog(app3, "销售订单", i[0]['FMRBBILLNO'],res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3, i[0]['FMRBBILLNO'], "2")
                        erro_num = erro_num + 1
                        erro_list.append(res)

            else:
                changeStatus(app3,i[0]['FMRBBILLNO'],"1")

        except Exception as e:

            insertLog(app3, "销售订单", i[0]['FMRBBILLNO'],"数据异常","2")

    dict={
        "sucessNum":sucess_num,
        "erroNum":erro_num,
        "erroList":erro_list
    }

    return dict

def check_order_exists(api_sdk,FNumber):
    '''
    查看订单是否在ERP系统存在
    :param api: API接口对象
    :param FNumber: 订单编码
    :return:
    '''



    model={
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

    res=json.loads(api_sdk.View("SAL_SaleOrder",model))

    return res['Result']['ResponseStatus']['IsSuccess']



def ERP_Submit(api_sdk,FNumber):
    '''
    将订单进行提交
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res=json.loads(api_sdk.Submit("SAL_SaleOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def ERP_Audit(api_sdk,FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "InterationFlags": "",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Audit("SAL_SaleOrder", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def unAudit(api_sdk,FNumber,option):
    '''
    将单据反审核
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    try:

        api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                           option['app_sec'], option['server_url'])

        model={
                "CreateOrgId": 0,
                "Numbers": [FNumber],
                "Ids": "",
                "InterationFlags": "",
                "IgnoreInterationFlag": "",
                "NetworkCtrl": "",
                "IsVerifyProcInst": ""
            }


        res=json.loads(api_sdk.UnAudit("SAL_SaleOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def delete(api_sdk,FNumber,option):
    '''
    将单据删除
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    try:

        api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                           option['app_sec'], option['server_url'])

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "NetworkCtrl": ""
        }

        res=json.loads(api_sdk.Delete("SAL_SaleOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql = "select distinct FMRBBILLNO,FIsFree from RDS_ECS_ODS_sal_returnstock where FIsdo=0 and FIsFree!=1"

    res = app3.select(sql)

    return res




def getClassfyData(app3, code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''

    sql=f"select FMRBBILLNO,FTRADENO,FSALEORDERENTRYSEQ,FBILLTYPE,FRETSALESTATE,FPRDRETURNSTATUS,OPTRPTENTRYDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FCUSTCODE,FPrdNumber,FPrdName,FRETSALEPRICE,FRETURNQTY,FREQUESTTIME,FBUSINESSDATE,FCOSTPRICE,FMEASUREUNIT,FRETAMOUNT,FTAXRATE,FLOT,FSALER,FAUXSALER,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FCHECKSTATUS,UPDATETIME,FDELIVERYNO,FIsDo,FIsFree,FADDID,FCurrencyName,FReturnTime from RDS_ECS_ODS_sal_returnstock where FMRBBILLNO='{code['FMRBBILLNO']}'"

    res=app3.select(sql)

    return res



def code_conversion(app2, tableName, param, param2):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    try:

        sql = f"select FNumber from {tableName} where {param}='{param2}'"

        res = app2.select(sql)

        if res == []:

            return ""

        else:

            return res[0]['FNumber']

    except Exception as e:

        return ""


def code_conversion_org(app2, tableName, param, param2, param3):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    try:

        sql = f"select FNumber from {tableName} where {param}='{param2}' and FOrgNumber='{param3}'"

        res = app2.select(sql)

        if res == []:

            return ""

        else:

            return res[0]['FNumber']

    except Exception as e:

        return ""


def changeStatus(app2, fnumber, status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    try:

        sql = f"update a set a.FIsDO={status} from RDS_ECS_ODS_Sales_Order a where FSALEORDERNO='{fnumber}'"

        app2.update(sql)

    except Exception as e:

        return ""


def getFinterId(app2, tableName):
    '''
    在两张表中找到最后一列数据的索引值
    :param app2: sql语句执行对象
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''

    try:

        sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"

        res = app2.select(sql)

        return res[0]['FMaxId']

    except Exception as e:

        return 0


def checkDataExist(app2, FSALEORDERNO):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FSALEORDERNO from RDS_ECS_SRC_Sales_Order where FSALEORDERNO='{FSALEORDERNO}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def deleteOldDate(app3,FNumber):
    '''
    删除旧数据
    :param app3:
    :param FNumber:
    :return:
    '''

    sql1=f"delete from RDS_ECS_SRC_Sales_Order where FSALEORDERNO='{FNumber}'"

    app3.delete(sql1)

    sql2 = f"delete from RDS_ECS_ODS_Sales_Order where FSALEORDERNO='{FNumber}'"

    app3.delete(sql2)

    return True


def IsUpdate(app3,seq,updateTime,api_sdk,option):

    # FStatus

    flag=False

    sql=f"select FSALEORDERNO,UPDATETIME,FStatus from RDS_ECS_SRC_Sales_Order where FSALEORDERENTRYSEQ='{seq}'"

    res=app3.select(sql)

    if res:

        if str(res[0]['FStatus'])=="待出货":

            try:

                if str(res[0]['UPDATETIME'])!=updateTime:

                    flag=True

                    deleteResult=deleteOldDate(app3,res[0]['FSALEORDERNO'])

                    if deleteResult:

                        unAuditResult=unAudit(api_sdk,res[0]['FSALEORDERNO'],option)

                        if unAuditResult:

                            delete(api_sdk,res[0]['FSALEORDERNO'],option)

                    else:

                        insertLog(app3, "销售订单",res[0]['FSALEORDERNO'], "反审核失败", "2")


            except Exception as e:

                insertLog(app3, "销售订单", res[0]['FSALEORDERNO'], "更新数据失败", "2")

                return False

        else:

            flag=False

    return flag




def insertLog(app2,FProgramName,FNumber,Message,FIsdo,cp='赛普'):
    '''
    异常数据日志
    :param app2:
    :param FNumber:
    :param Message:
    :return:
    '''

    sql="insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName,FIsdo) values('"+FProgramName+"','"+FNumber+"','"+Message+"',getdate(),'"+cp+"','"+FIsdo+"')"

    app2.insert(sql)

def classification_process(app3, data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res = fuz(app3, data)

    return res


def fuz(app3, codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''


    singleList = []

    for i in codeList:

        data = getClassfyData(app3, i)

        singleList.append(data)

    return singleList



def salesOrderRuturn(startDate, endDate,app2Toekn,app3Token,option):
    '''
    函数入口
    :param startDate:
    :param endDate:
    :return:
    '''

    # app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    app2 = RdClient(token=app2Toekn)
    app3 = RdClient(token=app3Token)

    api_sdk = K3CloudApiSdk()

    data = getCode(app3)

    if data!=[] :

        res = classification_process(app3, data)

        print(res)

        if res!=[]:

            msg=ERP_Save(api_sdk=api_sdk, data=res, option=option, app2=app2, app3=app3)

            return msg

    else:

        return {"message":"无订单需要同步"}











