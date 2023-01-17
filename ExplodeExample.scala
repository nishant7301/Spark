import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object ExplodeExample
{
def main(args:Array[String])
{
val spark=SparkSession.builder().master("local[4]").appName("Explode").getOrCreate();
val sc=spark.SparkContext;
val rdd=sc.textFile("C:/Users/Nishant Raj/Desktop/Spark/explode.txt");
val maprdd=rdd.map(f=>f.split(","))
val column=Seq("Name","mobileNumber")
val df=rdd.toDF();
val dfexplode=df.withColumn("name",$"value".getItem(0)).withColumn("number",explode(split($"value".getItem(1),":"))).drop("value");
}
}