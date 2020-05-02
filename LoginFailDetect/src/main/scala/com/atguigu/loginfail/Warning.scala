package com.atguigu.loginfail

// 输出数据格式
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)