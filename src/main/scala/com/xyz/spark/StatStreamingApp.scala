package com.xyz.spark

import com.xyz.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.xyz.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.xyz.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer
/**
  * Date: 2020-01-06 17:53
  * author: Mr.Shang
  *
  * description:测试sparkstreaming处理kafka过来的数据是否正常
  * 统计今天到现在为止课程的访问量,将统计结果保存到Hbase中
  * 统计从搜索引擎过来的今天到现在为止实战课程的访问量
  **/
object StatStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StatStreamingApp")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(3))
    //准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StatStreamingApp",
      //earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      //这里配置latest自动重置偏移量为最新的偏移量,即如果有偏移量从偏移量位置开始消费,没有偏移量从新来的数据开始消费
      "auto.offset.reset" -> "latest",
      //false表示关闭自动提交.由spark帮你提交到Checkpoint或程序员手动维护
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //定义topic
    val topics = Array("streamingtopic")

    // 配置kafka的偏移量
    val offsets = collection.Map[TopicPartition, Long] {
      new TopicPartition("streamingtopic", "0".toInt) -> 1604
    }


    //2.使用KafkaUtil连接Kafak获取数据
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,//位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
     // ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsets))//消费策略,源码强烈推荐使用该策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))//消费策略,源码强烈推荐使用该策略

    //3.操作数据,lineDStream的数据格式187.72.30.143	2020-01-05 15:40:38	"GET /class/146.html HTTP/1.1" 	200	https://www.baidu.com/s?wd=大数据面试
    val lineDStream: DStream[String] = recordDStream.map(_.value())
    // lineDStream.print()

    val cleanData= lineDStream.map(line => {
      val infos: Array[String] = line.split("\t")
      //url=>/class/112.html
      val url: String = infos(2).split(" ")(1)
      var courseId = 0

       //课程编号
          if (url.startsWith("/class")) {
            //146.html
            val courseIdHTML = url.split("/")(2)
            courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
          }
            ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
          }).filter(clicklog => clicklog.courseId != 0)
     cleanData.print()

    //统计今天到现在为止课程的访问量,将统计结果保存到Hbase中
    //将cleanData的结果转成对应的Hbase rowkey设计:20200108_123
    cleanData.map(data=>{

      (data.time.substring(0,8)+"_"+data.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair =>{
          list.append(CourseClickCount(pair._1,pair._2))
        })
        CourseClickCountDAO.save(list)
      })
    })

    //统计从搜索引擎过来的今天到现在为止实战课程的访问量
    cleanData.map(x => {

      /**
        * https://www.sogou.com/web?query=Spark SQL实战
        *
        * ==>
        *
        * https:/www.sogou.com/web?query=Spark SQL实战
        */
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()//开启
    ssc.awaitTermination()//等待优雅停止

  }

}
