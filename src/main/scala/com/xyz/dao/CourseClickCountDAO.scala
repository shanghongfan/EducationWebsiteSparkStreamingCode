package com.xyz.dao

import com.xyz.domain.CourseClickCount
import com.xyz.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer

/**
  * Date: 2020-01-08 18:59
  * author: Mr.Shang
  *
  * description: 课程点击数-数据访问层
  *
  **/
object CourseClickCountDAO {
  val tableName = "course_clickcount"
  val cf ="info"
  //列名
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    * @param list
    */
  def save(list:ListBuffer[CourseClickCount])={

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }


  /**
    * 根据rowkey查询值
    */
  def count(day_course: String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20200108_8",8))
    list.append(CourseClickCount("20200108_9",9))
    list.append(CourseClickCount("20200108_1",100))

    save(list)

    println(count("20200108_8") + " : " + count("20200108_9")+ " : " + count("20200108_1"))
  }
}
