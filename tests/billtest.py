#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mdlCpEcsBillpy import ecsbill_sync
from mdlCpEcsBillpy import ecsbill_sync2

# res = ecsbill_sync()
res = ecsbill_sync2("2023-07-27 00:00:00","2023-07-28 09:34:24")

print(res)

