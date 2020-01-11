package com.xyz.domain

/**
  * Date: 2020-01-06 20:08
  * author: Mr.Shang
  *
  * description: 课程点击数实体类
  *
  ** @param day_course 对应的就是HBase中的rowkey，20190108_1,1为课程编号
  ** @param click_count 对应的20190108_1的访问总数
  *
  **/
  case class CourseClickCount(day_course:String, click_count:Long)

