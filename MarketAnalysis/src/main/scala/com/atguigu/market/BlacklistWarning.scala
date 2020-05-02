package com.atguigu.market

// 黑名单的报警信息
case class BlacklistWarning(userId: Long, adId: Long, msg: String)