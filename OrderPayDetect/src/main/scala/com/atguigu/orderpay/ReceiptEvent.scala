package com.atguigu.orderpay

// 定义输入支付到帐事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)