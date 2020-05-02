package com.atguigu.orderpay

// 定义输入订单事件的样例类
case class OrderEvent(orderId: String, eventType: String, txId: String, eventTime: Long)