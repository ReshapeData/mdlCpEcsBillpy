#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpEcsBillpy import newFunc

from pyrda.dbms.rds import RdClient

if __name__ == '__main__':
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')
    app2 = RdClient(token='4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227')

    sql = "select * from RDS_ECS_src_Sales_Order where FSALEORDERNO='202307030001'"

    res=app3.select(sql)

    print(res)