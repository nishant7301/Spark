import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExample
{
 def main(args:Array[String])
 {
 val spark=SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate();
 val sc=spark.SparkContext;
 val rdd=sc.textFile("C:/Users/Nishant Raj/Desktop/Spark/test.txt");
 val flatrdd=rdd.flatMap(f=>f.split(" "));
 val maprdd=flatrdd.map(w=>(w,1));
 val filterrdd=maprdd.filter(a=>a._1.startsWith("a"))
 val reducerdd=filterrdd.reduceByKey(_+_);
 val sortRdd=reducerdd.map(m=>(m._2,m._1)).sortByKey();
 }
}