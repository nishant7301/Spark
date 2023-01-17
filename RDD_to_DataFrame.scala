import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import spark.implicits._

object RDD_to_DataFrame
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("Datafram").master("local[3]").getOrCreate();
val sc=spark.SparkContext
val rdd=sc.textFile("C:/Users/Nishant Raj/Desktop/Spark/data.txt");
val maprdd=rdd.map(f=>f.split(";/"));
val df=maprdd.toDF();
val df1=df.withColumn("empid",$"value".getItem(0)).withColumn("empname",$"value".getItem(1)).withColumn("managername",$"value".getItem(2)).drop("value")
val df2=df1.groupBy("managername").count()
}
}