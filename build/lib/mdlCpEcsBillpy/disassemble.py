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

        df = pd.DataFrame(info['data']['list'])

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

        return 0





def ERP_Save(app2,app3,api_sdk,option,data):
    '''
    组装拆卸单
    :return:
    '''

    try:

        erro_list = []
        sucess_num = 0
        erro_num = 0

        api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                           option['app_sec'], option['server_url'])

        for i in data:

            if check_order_exists(api_sdk,i['FBillNo'])!=True:

                model={
                        "Model": {
                            "FID": 0,
                            "FBillNo":str(i['FBillNo']),
                            "FBillTypeID": {
                                "FNUMBER": "ZZCX01_SYS"
                            },
                            "FStockOrgId": {
                                "FNumber": "104"
                            },
                            "FAffairType": "Dassembly",
                            "FDate": str(i['Fdate']),
                            "FDeptID": {
                                "FNumber": "BM000040"
                            },
                            "FSTOCKERID": {
                                "FNumber": "BSP00040"
                            },
                            "FSTOCKERGROUPID": {
                                "FNumber": "SKCKZ01"
                            },
                            "FOwnerTypeIdHead": "BD_OwnerOrg",
                            "FOwnerIdHead": {
                                "FNumber": "104"
                            },
                            "FSubProOwnTypeIdH": "BD_OwnerOrg",
                            "FSubProOwnerIdH": {
                                "FNumber": "104"
                            },
                            "FEntity": [
                                {
                                    "FMaterialID": {
                                        "FNumber": code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER",i['FItemNumber'])
                                    },
                                    # "FUnitID": {
                                    #     "FNumber": "01"
                                    # },
                                    "FQty": str(i['Fqty']),
                                    "FStockID": {
                                        "FNumber": "SK01" if i['FStockName']=='苏州总仓' or i['FStockName']=='样品仓' else code_conversion(app2,"rds_vw_warehouse","FNAME",i['FStockName'])
                                    },
                                    "FStockLocId": {
                                        "FSTOCKLOCID__FF100002": {
                                            "FNumber": "SK01" if i['FStockName']=='苏州总仓' or i['FStockName']=='样品仓' else code_conversion(app2,"rds_vw_warehouse","FNAME",i['FStockName'])
                                        }
                                    },
                                    "FStockStatusID": {
                                        "FNumber": "KCZT01_SYS"
                                    },
                                    "FLOT": {
                                        "FNumber": str(i['Flot']) if isbatch(app2,i['FItemNumber'])=='1' else ""
                                    },
                                    # "FBaseUnitID": {
                                    #     "FNumber": "01"
                                    # },
                                    "FRefBomID": {
                                        "FNumber": ""
                                    },
                                    "FOwnerTypeID": "BD_OwnerOrg",
                                    "FOwnerID": {
                                        "FNumber": "104"
                                    },
                                    "FKeeperTypeID": "BD_KeeperOrg",
                                    "FKeeperID": {
                                        "FNumber": "104"
                                    },
                                    "FProduceDate": str(i['FPRODUCEDATE']) if iskfperiod(app2,i['FItemNumber'])=='1' else "",
                                    "FEXPIRYDATE": str(i['FEFFECTIVEDATE']) if iskfperiod(app2,i['FItemNumber'])=='1' else "",
                                    # "FInstockDate": "2022-11-12 00:00:00",
                                    "FSubEntity": data_splicing(app2,app3,i['FBillNo'])
                                }
                            ]
                        }
                    }

                save_res=json.loads(api_sdk.Save("STK_AssembledApp",model))

                if save_res['Result']['ResponseStatus']['IsSuccess']:

                    submit_res=ERP_submit(api_sdk,i['FBillNo'])

                    if submit_res:

                        audit_res=ERP_Audit(api_sdk,i['FBillNo'])

                        if audit_res:

                            changeStatus(app3,i['FBillNo'],"1")

                            sucess_num=sucess_num+1

                    else:
                        pass
                else:

                    insertLog(app3, "组装拆卸单数据异常", i['FBillNo'],
                                 save_res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                    changeStatus(app3, i['FBillNo'], "2")

                    erro_num=erro_num+1

                    erro_list.append(save_res)

            else:
                insertLog(app3, "组装拆卸单数据异常", i['FBillNo'],"数据同步成功", "1")

                changeStatus(app3, i['FBillNo'], "1")

        dict = {
            "sucessNum": sucess_num,
            "erroNum": erro_num,
            "erroList": erro_list
        }
        return dict

    except Exception as e:

        return {"message":"订单异常"}



def check_order_exists(api_sdk,FNumber):
    '''
    查看订单是否在ERP系统存在
    :param api: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
                "CreateOrgId": 0,
                "Number": FNumber,
                "Id": "",
                "IsSortBySeq": "false"
            }

        res=json.loads(api_sdk.View("STK_AssembledApp",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True


def ERP_submit(api_sdk, FNumber):

    try:

        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Submit("STK_AssembledApp", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def ERP_Audit(api_sdk, FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "InterationFlags": "",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": "",
        }

        res = json.loads(api_sdk.Audit("STK_AssembledApp", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def json_model(app2,i):
    '''
    子件数据模型
    :param app2:
    :param i:
    :return:
    '''

    try:

        model={
                "FMaterialIDSETY": {
                    "FNumber": code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER",i['FItemNumber'])
                },
                # "FUnitIDSETY": {
                #     "FNumber": "01"
                # },
                "FQtySETY": str(i['Fqty']),
                "FStockIDSETY": {
                    "FNumber": "SK01" if i['FStockName']=='苏州总仓' or i['FStockName']=='样品仓' else code_conversion(app2,"rds_vw_warehouse","FNAME",i['FStockName'])
                },
                "FStockLocIdSETY": {
                    "FSTOCKLOCIDSETY__FF100002": {
                        "FNumber": "SK01" if i['FStockName']=='苏州总仓' or i['FStockName']=='样品仓' else code_conversion(app2,"rds_vw_warehouse","FNAME",i['FStockName'])
                    }
                },
                "FStockStatusIDSETY": {
                    "FNumber": "KCZT01_SYS"
                },
                "FLOTSETY": {
                    "FNumber": str(i['Flot']) if isbatch(app2,i['FItemNumber'])=='1' else ""
                },
                # "FBaseUnitIDSETY": {
                #     "FNumber": "01"
                # },
                "FKeeperTypeIDSETY": "BD_KeeperOrg",
                "FKeeperIDSETY": {
                    "FNumber": "104"
                },
                "FOwnerTypeIDSETY": "BD_OwnerOrg",
                "FOwnerIDSETY": {
                    "FNumber": "104"
                },
                "FProduceDateSETY": str(i['FPRODUCEDATE']) if iskfperiod(app2,i['FItemNumber'])=='1' else "",
                "FEXPIRYDATESETY": str(i['FEFFECTIVEDATE']) if iskfperiod(app2,i['FItemNumber'])=='1' else "",
            }


        return model

    except Exception as e:

        return {}

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    try:

        sql="select FBillNo,Fseq,Fdate,FDeptName,FItemNumber,FItemName,FItemModel,FUnitName,Fqty,FStockName,Flot,Fnote,FPRODUCEDATE,FEFFECTIVEDATE,FSUMSUPPLIERLOT,FAFFAIRTYPE,FIsdo from RDS_ECS_ODS_DISASS_DELIVERY where FIsdo=0 and FAFFAIRTYPE='拆卸'"

        res=app3.select(sql)

        return res

    except Exception as e:

        return []

def code_conversion(app2,tableName,param,param2):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql=f"select FNumber from {tableName} where {param}='{param2}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FNumber']

def iskfperiod(app2,FNumber):
    '''
    查看物料是否启用保质期
    :param app2:
    :param FNumber:
    :return:
    '''

    sql=f"select FISKFPERIOD from rds_vw_fiskfperiod where F_SZSP_SKUNUMBER='{FNumber}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FISKFPERIOD']


def getSubEntityCode(app2,FNumber):
    '''
    获得子件信息
    :return:
    '''

    sql=f"select * from RDS_ECS_ODS_ASS_STORAGEACCT where FBillNo='{FNumber}'"

    res=app2.select(sql)

    return res

def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.FIsDo={status} from RDS_ECS_ODS_DISASS_DELIVERY a where a.FBillNo='{fnumber}'"

    app3.update(sql)

def checkDataExist(app2, tableName,FBillNo):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FBillNo from {tableName} where FBillNo='{FBillNo}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def getFinterId(app2,tableName):
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

def insert_assembly_order(app3,data):
    '''
    组装单
    :param app2:
    :param data:
    :return:
    '''


    for i in data.index:

        try:

            if checkDataExist(app3,"RDS_ECS_SRC_ASS_STORAGEACCT",data.loc[i]['FBillNo']):

                sql = f"""insert into RDS_ECS_SRC_ASS_STORAGEACCT(FInterID,FBillNo,Fseq,Fdate,FDeptName,FItemNumber,FItemName,FItemModel,FUnitName,Fqty,FStockName,Flot,FBomNumber,FNote,FPRODUCEDATE,FEFFECTIVEDATE,FSUMSUPPLIERLOT,FAFFAIRTYPE) values({int(getFinterId(app3,'RDS_ECS_SRC_ASS_STORAGEACCT'))+1},'{data.loc[i]['FBillNo']}','{data.loc[i]['Fseq']}','{data.loc[i]['Fdate']}','{data.loc[i]['FDeptName']}','{data.loc[i]['FItemNumber']}','{data.loc[i]['FItemName'].replace("'","''")}','{data.loc[i]['FItemModel']}','{data.loc[i]['FUnitName']}','{data.loc[i]['Fqty']}','{data.loc[i]['FStockName']}','{data.loc[i]['Flot']}','{data.loc[i]['FBomNumber']}','{data.loc[i]['Fnote']}','{data.loc[i]['FPRODUCEDATE']}','{data.loc[i]['FEFFECTIVEDATE']}','{data.loc[i]['FSUMSUPPLIERLOT']}','{data.loc[i]['FAFFAIRTYPE']}')"""

                app3.insert(sql)

        except Exception as e:

            insertLog(app3, "组装拆卸单数据插入SRC", data.loc[i]['FBillNo'], "数据异常，请检查数据","2")



def insert_remove_order(app3,data):
    '''
    拆卸单
    :param app2:
    :param data:
    :return:
    '''


    for i in data.index:

        if checkDataExist(app3,"RDS_ECS_SRC_DISASS_DELIVERY",data.loc[i]['FBillNo']):

            sql = f"""insert into RDS_ECS_SRC_DISASS_DELIVERY(FInterID,FBillNo,Fseq,Fdate,FDeptName,FItemNumber,FItemName,FItemModel,FUnitName,Fqty,FStockName,Flot,FNote,FPRODUCEDATE,FEFFECTIVEDATE,FSUMSUPPLIERLOT,FAFFAIRTYPE) values({int(getFinterId(app3,'RDS_ECS_SRC_DISASS_DELIVERY'))+1},'{data.loc[i]['FBillNo']}','{data.loc[i]['Fseq']}','{data.loc[i]['Fdate']}','{data.loc[i]['FDeptName']}','{data.loc[i]['FItemNumber']}','{data.loc[i]['FItemName'].replace("'","''")}','{data.loc[i]['FItemModel']}','{data.loc[i]['FUnitName']}','{data.loc[i]['Fqty']}','{data.loc[i]['FStockName']}','{data.loc[i]['Flot']}','{data.loc[i]['Fnote']}','{data.loc[i]['FPRODUCEDATE']}','{data.loc[i]['FEFFECTIVEDATE']}','{data.loc[i]['FSUMSUPPLIERLOT']}','{data.loc[i]['FAFFAIRTYPE']}')"""

            app3.insert(sql)




def isbatch(app2,FNumber):

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


def insertLog(app2,FProgramName,FNumber,Message,FIsdo,cp='赛普'):
    '''
    异常数据日志
    :param app2:
    :param FNumber:
    :param Message:
    :return:
    '''

    sql="insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName,FIsdo) values('"+FProgramName+"','"+FNumber+"','"+Message+"',getdate(),'"+cp+"','"+str(FIsdo)+"')"

    app2.insert(sql)




def data_splicing(app2,app3,FNumber):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data:
    :return:
    '''

    data=getSubEntityCode(app3,FNumber)

    list=[]

    for i in data:

        list.append(json_model(app2,i))

    return list


def writeSRCA(startDate, endDate, app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_processing_storage", startDate, endDate, "Fdate")

    if page:

        for i in range(1, int(page) + 1):

            df = ECS_post_info2(url, i, 1000, "ge", "le", "v_processing_storage", startDate, endDate, "Fdate")

            df = df.fillna("")

            insert_assembly_order(app3, df)

    pass

def writeSRCD(startDate, endDate, app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page =viewPage(url, 1, 1000, "ge", "le", "v_disassemble_discharge", startDate, endDate, "Fdate")

    if page:

        for i in range(1, int(page) + 1):

            df = ECS_post_info2(url, i, 1000, "ge", "le", "v_disassemble_discharge", startDate, endDate, "Fdate")

            df = df.fillna("")

            insert_remove_order(app3, df)



def assemblyDis(startDate,endDate,app2,app3,option):

    # app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    # app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')

    writeSRCA(startDate,endDate,app3)
    writeSRCD(startDate,endDate,app3)

    res = getCode(app3)

    if res:

        api_sdk = K3CloudApiSdk()

        msg=ERP_Save(app2=app2, app3=app3, api_sdk=api_sdk, option=option, data=res)

        return msg
    else:
        return {"message":"无订单需要同步"}


def classification_process(app3,data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res=getClassfyData(app3,data)

    # res=fuz(app3,data)

    return res

def getClassfyData(app3,code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''

    sql = f"""select FBillNo,Fseq,Fdate,FDeptName,FItemNumber,FItemName,FItemModel,FUnitName,Fqty,FStockName,Flot,Fnote,FPRODUCEDATE,FEFFECTIVEDATE,FSUMSUPPLIERLOT,FAFFAIRTYPE,FIsdo from RDS_ECS_ODS_DISASS_DELIVERY where FBillNo='{code['FBillNo']}'"""

    res = app3.select(sql)

    return res



def fuz(app3,codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''

    singleList=[]

    for i in codeList:

        data=getClassfyData(app3,i)
        singleList.append(data)


    return singleList

def assemblyDis_byOrder(app2,app3,option,data):
    '''
    按单据同步
    :param startDate:
    :param endDate:
    :return:
    '''

    api_sdk = K3CloudApiSdk()


    if data!=[] :

        res = classification_process(app3, data[0])

        if res!=[]:

            msg=ERP_Save(api_sdk=api_sdk, data=res, option=option, app2=app2, app3=app3)

            return msg

    else:

        return {"message":"SRC无此订单"}

