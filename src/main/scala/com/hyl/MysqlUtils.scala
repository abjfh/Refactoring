package com.hyl

import java.util.Properties


/**
 * @version: java version 1.8
 * @version: scala version 2.12
 * @Author: hyl
 * @description:
 * @date: 2024-01-10 3:04
 */
object MysqlUtils {
  val SQL_IP_PORT = "localhost:3306"
  val SQL_DB_NAME = "enterprise"
  // 设置支持批量操作
  val SQL_BATCH_PARAM = "rewriteBatchedStatements=true"
  val SQL_DB_MARKET_URL: String = s"jdbc:mysql://${SQL_IP_PORT}/${SQL_DB_NAME}?${SQL_BATCH_PARAM}"
  val SQL_DB_USERNAME = "root"
  val SQL_DB_PASSWORD = "123456"
  val UTF8 = "UTF-8"

  def getDBProps(): Properties = {
    val props = new Properties()
    props.put("user", SQL_DB_USERNAME)
    props.put("password", SQL_DB_PASSWORD)
    props.setProperty("useSSL", "false")
    props.setProperty("useUnicode", "true")
    props.setProperty("characterEncoding", UTF8)
    props
  }
}
