import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType,StructType,StructField,Integer}
import spark.implicits._

object DataFrameExample
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("DataFrameTest").master("local[4]").getOrCreate();
val data=Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"));
val column=Seq("language","users_count");
val sc=spark.SparkContext();
val rdd=sc.parallelize(data);
val df=rdd.toDF(column:_*);
val df1=spark.createDataFrame(rdd).toDF(column:_*);
val rddrow=rdd.map(f=>Row(f._1,f._2))
val schema=StructType(Array(
StructField("language",StringType,true),
StructField("users_count",StringType,true)
))
val dfrow=spark.createDataFrame(rddrow,schema);

val df_collection=data.toDF();
val df_collection=data.toDF(column:_*);
}
}
