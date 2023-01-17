import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ColeaseExample
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("Test").master("localhost["1"]").getOrCreate();
val sc=spark.SparkContext;
val df=spark.read.option("inferSchema","true").option("header","true").csv("C:/Users/Nishant Raj/Desktop/Spark/name.txt");
val df1=df.withColumn("firstName",split($"Name"," ").getItem(0)).withColumn("Middlename1",split($"Name"," ").getItem(1)).withColumn("Middlename2",split($"Name"," ").getItem(2)).withColumn("lastName",split($"Name"," ").getItem(3));
val df2=df1.withColumn("lastName",coalesce(col("LastName"),col("Middlename2"),col("Middlename1"))).drop("Middlename1").drop("Middlename2")
df2.show();
}
}