import com.hyl.MysqlUtils.{SQL_DB_MARKET_URL, getDBProps}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.junit.Test

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

/**
 * @version: java version 1.8
 * @version: scala version 2.12
 * @Author: hyl
 * @description:
 * @date: 2024-01-10 12:23
 */
class test01 {

  private val session = SparkSession
    .builder
    .appName("test structure")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.scheduler.mode", "FAIR")
    .getOrCreate()
  private val df = session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    .option("kafka.group.id", "Enterprise_consumer")
    .option("subscribe", "Enterprise")
    .option("startingOffsets", "latest")
    .load()

  private val structType = new StructType(
    Array(
      StructField("bianh", StringType),
      StructField("com_name", StringType),
      StructField("com_addr", StringType),
      StructField("cat", StringType),
      StructField("se_cat", StringType),
      StructField("com_des", StringType),
      StructField("born_data", TimestampType),
      StructField("death_data", TimestampType),
      StructField("live_days", StringType),
      StructField("financing", StringType),
      StructField("total_money", StringType),
      StructField("death_reason", StringType),
      StructField("invest_name", StringType),
      StructField("ceo_name", StringType),
      StructField("ceo_des", StringType),
      StructField("ceo_per_des", StringType)
    )
  )

  import session.implicits._

  private val dataSource = df.selectExpr("cast(value as string)")
    .withColumn("value", from_json($"value", structType))
    .select("value.*")
    .withColumn("live_days", $"live_days".cast(IntegerType))
    .withColumn("total_money", $"total_money".cast(IntegerType))

  @Test
  def fun01: Unit = {
    val com_addr_inc = dataSource
      .groupBy(
        window($"death_data", "365 days", "365 days"),
        $"com_addr"
      ).agg(
        count("*").as("addr_cnt_one_year"),
        floor(avg("live_days")).as("addr_avg_live_days_one_year")
      )
      .selectExpr(
        "window.*",
        "com_addr",
        "addr_cnt_one_year",
        "addr_avg_live_days_one_year"
      )
      .na.fill(0L, Array("addr_cnt_one_year", "addr_avg_live_days_one_year"))
    com_addr_inc.printSchema()

    com_addr_inc.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO com_addr_inc " +
                s"(start, end,com_addr,addr_cnt_one_year,addr_avg_live_days_one_year) " +
                s"VALUES(?, ?, ?, ?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE start=?, end=?,com_addr=?,addr_cnt_one_year=?,addr_avg_live_days_one_year=?;")
            con != null
          }

          override def process(value: Row): Unit = {
            val start = value.getAs[Timestamp](0)
            val end = value.getAs[Timestamp](1)
            val com_addr = value.getAs[String](2)
            val addr_cnt_one_year = value.getAs[Long](3)
            val addr_avg_live_days_one_year = value.getAs[Long](4)


            prp.setTimestamp(1, start)
            prp.setTimestamp(2, end)
            prp.setString(3, com_addr)
            prp.setLong(4, addr_cnt_one_year)
            prp.setLong(5, addr_avg_live_days_one_year)


            prp.setTimestamp(6, start)
            prp.setTimestamp(7, end)
            prp.setString(8, com_addr)
            prp.setLong(9, addr_cnt_one_year)
            prp.setLong(10, addr_avg_live_days_one_year)
            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

    session.streams.awaitAnyTermination()
  }

  @Test
  def fun02 = {
    //    计算各个省份总计倒闭的企业数量和企业的平均live_days
    val com_addr_full = dataSource
      .groupBy(
        $"com_addr"
      ).agg(
        count("*").as("addr_cnt_history"),
        floor(avg("live_days")).as("addr_avg_live_days")
      )
    com_addr_full.printSchema()
    com_addr_full.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO com_addr_full " +
                s"(com_addr,addr_cnt_history,addr_avg_live_days) " +
                s"VALUES(?, ?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE com_addr=?,addr_cnt_history=?,addr_avg_live_days=?;")
            con != null
          }

          override def process(value: Row): Unit = {
            val com_addr = value.getAs[String](0)
            val addr_cnt_history = value.getAs[Long](1)
            val addr_avg_live_days = value.getAs[Long](2)

            prp.setString(1, com_addr)
            prp.setLong(2, addr_cnt_history)
            prp.setLong(3, addr_avg_live_days)

            prp.setString(4, com_addr)
            prp.setLong(5, addr_cnt_history)
            prp.setLong(6, addr_avg_live_days)

            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()
    session.streams.awaitAnyTermination()
  }

  @Test
  def fun03 = {
    dataSource.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }


  @Test
  def fun04 = {
    //    计算每年，各个省份倒闭的企业数量和企业的平均live_days
    val com_addr_inc = dataSource
      .groupBy(
        window($"death_data", "365 days", "365 days"),
        $"com_addr"
      ).agg(
        count("*").as("addr_cnt_one_year"),
        floor(avg("live_days")).as("addr_avg_live_days_one_year")
      )
      .selectExpr(
        "window.*",
        "com_addr",
        "addr_cnt_one_year",
        "addr_avg_live_days_one_year"
      )
      .na.fill(0L, Array("addr_cnt_one_year", "addr_avg_live_days_one_year"))
    com_addr_inc.printSchema()
    com_addr_inc.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO com_addr_inc " +
                s"(start, end,com_addr,addr_cnt_one_year,addr_avg_live_days_one_year) " +
                s"VALUES(?, ?, ?, ?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE start=?, end=?,com_addr=?,addr_cnt_one_year=?,addr_avg_live_days_one_year=?;")
            con != null
          }

          override def process(value: Row): Unit = {
            val start = value.getAs[Timestamp](0)
            val end = value.getAs[Timestamp](1)
            val com_addr = value.getAs[String](2)
            val addr_cnt_one_year = value.getAs[Long](3)
            val addr_avg_live_days_one_year = value.getAs[Long](4)


            prp.setTimestamp(1, start)
            prp.setTimestamp(2, end)
            prp.setString(3, com_addr)
            prp.setLong(4, addr_cnt_one_year)
            prp.setLong(5, addr_avg_live_days_one_year)


            prp.setTimestamp(6, start)
            prp.setTimestamp(7, end)
            prp.setString(8, com_addr)
            prp.setLong(9, addr_cnt_one_year)
            prp.setLong(10, addr_avg_live_days_one_year)
            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

      .awaitTermination()
  }

  @Test
  def fun06 = {
    //    计算各个省份总计倒闭的企业数量和企业的平均live_days
    val com_addr_full = dataSource
      .groupBy(
        $"com_addr"
      ).agg(
        count("*").as("addr_cnt_history"),
        floor(avg("live_days")).as("addr_avg_live_days")
      )
    com_addr_full.printSchema()
    com_addr_full.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO com_addr_full " +
                s"(com_addr,addr_cnt_history,addr_avg_live_days) " +
                s"VALUES(?, ?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE com_addr=?,addr_cnt_history=?,addr_avg_live_days=?;")
            con != null
          }

          override def process(value: Row): Unit = {
            prp.setString(1, value.getString(0))
            prp.setLong(2, value.getLong(1))
            prp.setLong(3, value.getLong(2))

            prp.setString(4, value.getString(0))
            prp.setLong(5, value.getLong(1))
            prp.setLong(6, value.getLong(2))

            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

      .awaitTermination()
  }

  @Test
  def fun07 = {

    val se_cat_inc = dataSource
      .groupBy(
        window($"death_data", "365 days", "365 days"),
        $"cat",
        $"se_cat"
      ).agg(
        count("*").as("se_cat_cnt_one_year"),
        floor(avg("live_days")).as("se_cat_avg_live_days_one_year"),
        sum("total_money").as("se_cat_total_money_one_year")
      )
      .select(
        "window.*",
        "cat",
        "se_cat",
        "se_cat_cnt_one_year",
        "se_cat_avg_live_days_one_year",
        "se_cat_total_money_one_year"
      )
      .na.fill("Unknown", Array("cat", "se_cat"))
      .na.fill(0, Array("se_cat_cnt_one_year", "se_cat_avg_live_days_one_year", "se_cat_total_money_one_year"))
    se_cat_inc.printSchema()
    se_cat_inc.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO se_cat_inc " +
                s"(start,end,cat,se_cat,se_cat_cnt_one_year,se_cat_avg_live_days_one_year,se_cat_total_money_one_year) " +
                s"VALUES(?, ?, ?, ?, ?,?,?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE start=?,end=?,cat=?,se_cat=?,se_cat_avg_live_days_one_year=?,se_cat_avg_live_days_one_year=?,se_cat_total_money_one_year=?;")
            con != null
          }

          override def process(value: Row): Unit = {
            prp.setTimestamp(1, value.getTimestamp(0))
            prp.setTimestamp(2, value.getTimestamp(1))
            prp.setString(3, value.getString(2))
            prp.setString(4, value.getString(3))
            prp.setLong(5, value.getLong(4))
            prp.setLong(6, value.getLong(5))
            prp.setLong(7, value.getLong(6))

            prp.setTimestamp(8, value.getTimestamp(0))
            prp.setTimestamp(9, value.getTimestamp(1))
            prp.setString(10, value.getString(2))
            prp.setString(11, value.getString(3))
            prp.setLong(12, value.getLong(4))
            prp.setLong(13, value.getLong(5))
            prp.setLong(14, value.getLong(6))

            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

      .awaitTermination()
  }

  @Test
  def fun08 = {
    val se_cat_full = dataSource
      .groupBy(
        $"cat",
        $"se_cat"
      ).agg(
        count("*").as("se_cat_cnt_history"),
        floor(avg("live_days")).as("se_cat_avg_live_days"),
        sum("total_money").as("se_cat_total_money")
      )
      .na.fill("Unknown", Array("cat", "se_cat"))
      .na.fill(0, Array("se_cat_cnt_history", "se_cat_avg_live_days", "se_cat_total_money"))
    se_cat_full.printSchema()
    se_cat_full.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO se_cat_full " +
                s"(cat,se_cat,se_cat_cnt_history,se_cat_avg_live_days,se_cat_total_money) " +
                s"VALUES(?, ?, ?, ?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE cat=?,se_cat=?,se_cat_cnt_history=?,se_cat_avg_live_days=?,se_cat_total_money=?;")
            con != null
          }

          override def process(value: Row): Unit = {

            prp.setString(1, value.getString(0))
            prp.setString(2, value.getString(1))
            prp.setLong(3, value.getLong(2))
            prp.setLong(4, value.getLong(3))
            prp.setLong(5, value.getLong(4))
            prp.setString(6, value.getString(0))
            prp.setString(7, value.getString(1))
            prp.setLong(8, value.getLong(2))
            prp.setLong(9, value.getLong(3))
            prp.setLong(10, value.getLong(4))
            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()


      .awaitTermination()
  }

  @Test
  def fun9 = {
    //公司死亡的原因和数量
    val death_reason_full = dataSource
      .select("death_reason")
      .na.drop()
      .flatMap(row => {
        row.getString(0).split(" ")
      })
      .withColumnRenamed("value", "death_reason")
      .groupBy("death_reason")
      .agg(
        count("*").as("cnt_death_reason")
      )
    death_reason_full.printSchema()
    death_reason_full.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO death_reason_full " +
                s"(death_reason,cnt_death_reason) " +
                s"VALUES(?, ?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE death_reason=?,cnt_death_reason=?;")
            con != null
          }

          override def process(value: Row): Unit = {

            prp.setString(1, value.getString(0))
            prp.setLong(2, value.getLong(1))

            prp.setString(3, value.getString(0))
            prp.setLong(4, value.getLong(1))
            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

      .awaitTermination()
  }

  @Test
  def fun10 = {
    //公司死亡的原因和数量在时间上的分布
    val death_reason_inc = dataSource
      .select("death_reason", "death_data")
      .na.drop("any")
      .flatMap(row => {
        val death_data = row.getTimestamp(1)
        row.getString(0).split(" ")
          .map((_, death_data))
      })
      .toDF("death_reason", "death_data")
      .groupBy(
        window($"death_data", "365 days", "365 days"),
        $"death_reason"
      )
      .agg(
        count("*").as("cnt_death_reason_one_year")
      )
      .select(
        "window.*",
        "death_reason",
        "cnt_death_reason_one_year",
      )
    death_reason_inc.printSchema()
    death_reason_inc.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO death_reason_inc " +
                s"(start,end,death_reason,cnt_death_reason_one_year) " +
                s"VALUES(?, ?,?,?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE start=?,end=?,death_reason=?,cnt_death_reason_one_year=?;")
            con != null
          }

          override def process(value: Row): Unit = {

            prp.setTimestamp(1, value.getTimestamp(0))
            prp.setTimestamp(2, value.getTimestamp(1))
            prp.setString(3, value.getString(2))
            prp.setLong(4, value.getLong(3))

            prp.setTimestamp(5, value.getTimestamp(0))
            prp.setTimestamp(6, value.getTimestamp(1))
            prp.setString(7, value.getString(2))
            prp.setLong(8, value.getLong(3))


            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()


      .awaitTermination()
  }

  @Test
  def fun11 = {
    //    投资机构的投资总额,投资过的企业的平均生存时间 区域折线图
    val invest_full = dataSource
      .select(
        "invest_name",
        "total_money",
        "live_days"
      )
      .na.drop("any")
      .flatMap(
        row => {
          val invest_names = row.getString(0).split("&")
          val total_money = row.getInt(1)
          val live_days = row.getInt(2)
          val avg_invest_money = total_money / invest_names.size
          invest_names.map(
            (_, avg_invest_money, live_days)
          )
        }
      ).toDF("invest_names", "avg_invest_money", "live_days")
      .groupBy(
        "invest_names"
      )
      .agg(
        sum("avg_invest_money").as("invest_total_money"),
        floor(avg("live_days")).as("invest_com_avg_live_days")
      )
    invest_full.printSchema()
    invest_full.writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Row] {
          var con: Connection = null
          var prp: PreparedStatement = null

          override def open(partitionId: Long, epochId: Long): Boolean = {
            con = DriverManager.getConnection(SQL_DB_MARKET_URL, getDBProps())

            prp = con.prepareStatement(
              s"INSERT INTO invest_full " +
                s"(invest_names,invest_total_money,invest_com_avg_live_days) " +
                s"VALUES(?, ?,?) " +
                s"ON DUPLICATE KEY " +
                s"UPDATE invest_names=?,invest_total_money=?,invest_com_avg_live_days=?;")
            con != null
          }

          override def process(value: Row): Unit = {


            prp.setString(1, value.getString(0))
            prp.setLong(2, value.getLong(1))
            prp.setDouble(3, value.getDouble(2))

            prp.setString(4, value.getString(0))
            prp.setLong(5, value.getLong(1))
            prp.setDouble(6, value.getDouble(2))


            prp.execute()
            prp.clearParameters()
          }

          override def close(errorOrNull: Throwable): Unit = {
            prp.close()
            con.close()
          }
        }
      ).start()

      .awaitTermination()
  }


}
