package org.fxi.test.spark.utils

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by xifei on 15-10-9.
 */
object FileUtils {

  val TEXT_LINE = "\r\n";
  val TEXT_TAB = "  ";
  val TEXT_COMM = ",";
  val YYYYMMDD = new SimpleDateFormat("yyyy_MM_dd")
  val YYYYMMDD_ = new SimpleDateFormat("yyyy-MM-dd")

  def saveToFile(path: String, array: Array[Row]): Unit = {
    var sb = new StringBuffer()
    var count:Int = 0;
    for(r <- array){
      sb.append(r.toString().replace("[","").replace("]","").replace(TEXT_COMM,TEXT_TAB))
      sb.append(TEXT_LINE)

      if(count % 5000 == 0 ){
        saveToFile(path,sb.toString)
        sb = null;
        sb = new StringBuffer();
      }

      count=count+1;
    }

    saveToFile(path,sb.toString)

  }

  def saveToFile[T](path: String, array: Array[T]): Unit = {
    var sb = new StringBuffer()
    var count:Int = 0;
    for(r <- array){
      sb.append(r.toString())
      sb.append(TEXT_LINE)

      if(count % 5000 == 0 ){
        saveToFile(path,sb.toString)
        sb = null;
        sb = new StringBuffer();
      }

      count=count+1;
    }

    saveToFile(path,sb.toString)

  }

  def saveToFile(path: String, context: String): Unit = {
    val file = new File(path);
    val parent = file.getParentFile;
    if(parent != null && !parent.exists()){
      parent.mkdirs()
    }

    if(!file.exists()){
      file.createNewFile()
    }else {
      file.delete()
    }

    var out:BufferedWriter = null;
    try{
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)))
      out.write(context.toString())
    } finally {
      if(out !=null){
        out.close();
      }
    }
  }

  def saveToFile(path: String, context: Array[Any]): Unit = {
    val file = new File(path);
    val parent = file.getParentFile;
    if(parent != null && !parent.exists()){
      parent.mkdirs()
    }

    if(!file.exists()){
      file.createNewFile()
    }

    var out:BufferedWriter = null;
    try{
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)))
      out.write(context.mkString("\n"))
    } finally {
      if(out !=null){
        out.close();
      }
    }
  }

  def saveToFile(path: String, df: DataFrame): Unit = {
    saveToFile(path , df.collect())
  }

  def toDayInt( cal:Calendar):Int = {
    cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH) + 1) * 100 + cal.get(Calendar.DAY_OF_MONTH)
  }

  def toDayInt( date : Date):Int= {
    val cal = Calendar.getInstance();
    cal.setTime(date);
    return toDayInt(cal);
  }

  def getDayArray(startDay:String ,endDay:String):Array[Long] ={
    val startCal = Calendar.getInstance()
    startCal.setTime(YYYYMMDD.parse(startDay))

    val endCal = Calendar.getInstance()
    endCal.setTime(YYYYMMDD.parse(endDay))

    val dayList = new util.ArrayList[Long];

    while(startCal.before(endCal)){
      dayList.add(toDayInt(startCal))
      startCal.add(Calendar.DAY_OF_YEAR, 1);
    }

    val rsArr = new Array[Long](dayList.size());
    for(i <- 0 to  (dayList.size() -1) ){
      rsArr(i) = dayList.get(i)
    }
    rsArr
  }

  def main(args: Array[String]): Unit = {
    for( a <- getDayArray("2015_10_09","2015_11_10")){
      println(a)
    }
  }
}
