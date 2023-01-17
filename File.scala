import org.apache.spark.sql.SparkSession
import org.apache.rdd.RDD
object File
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("FileTest").master("local[1]").getOrCreate();
val sc=spark.SparkContext
val rdd=sc.textFile("C:/Users/Nishant Raj/Desktop/Spark/*");
rdd.collect.foreach(f=>println(f));
}
}
