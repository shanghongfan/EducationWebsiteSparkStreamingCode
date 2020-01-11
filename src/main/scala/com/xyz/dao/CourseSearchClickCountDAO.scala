package com.xyz.dao
import com.xyz.domain.{CourseClickCount, CourseSearchClickCount}
import com.xyz.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
/**
  * Date: 2020-01-11 17:15
  * author: Mr.Shang
  *
  * description: 从搜索引擎过来的实战课程点击数-数据访问层
  *
  **/
object CourseSearchClickCountDAO {
  val tableName = "course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"


/**
  * 保存数据到HBase
  *
  * @param list  CourseSearchClickCount集合
  */
def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

  val table = HBaseUtils.getInstance().getTable(tableName)

  for(ele <- list) {
  table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
  Bytes.toBytes(cf),
  Bytes.toBytes(qualifer),
  ele.click_count)
}

}


  /**
    * 根据rowkey查询值
    */
  def count(day_search_course: String):Long = {
  val table = HBaseUtils.getInstance().getTable(tableName)

  val get = new Get(Bytes.toBytes(day_search_course))
  val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

  if(value == null) {
  0L
}else{
  Bytes.toLong(value)
}
}

  def main(args: Array[String]): Unit = {

  val list = new ListBuffer[CourseSearchClickCount]
  list.append(CourseSearchClickCount("20200108_www.baidu.com_8",8))
  list.append(CourseSearchClickCount("20200108_cn.bing.com_9",9))

  save(list)

  println(count("20200108_www.baidu.com_8") + " : " + count("20200108_cn.bing.com_9"))
}

}

