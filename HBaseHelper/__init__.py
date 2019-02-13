# -*- coding: utf-8 -*-

import happybase

pool = happybase.ConnectionPool(host='localhost',port=9090,size=10)
