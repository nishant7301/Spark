import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WithColumn
{
df main(args:Array[String])
{
val spark=SparkSession.builder().master("localhost["4"]").appName("WithColumnTest").getOrCreate();

val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
);
val withColumnschema=new StructType().add("name", new StructType().add("firstname",StringType).add("middlename",StringType).add("lastname",StringType)).add("dob",StringType).add("gender",StringType).add("salary",StringType)
val rdd=sc.parallelize(data);
val df=spark.createDataFrame(rdd, withColumnschema);
val new_df=df.withColumn("country",lit("India")); //Add  New Column
new_df.withColumn("salary",col("salary")*10).show() // update Existing column
new_df.withColumn("new_salary",col("salary")*10).show() // update Existing column
new_df.withColumn("salary",col("salary").cast("Integer")).show() // change data type

val data1 = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"), 
             ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"));//split column
val columns=Seq("name","address");
val rdd=sc.parallelize(data1);
val df=spark.createDataFrame(rdd).toDF(columns:_*)
df.withColumn("firstname",split($"name",",").getItem(0)).withColumn("lastname",split($"name",",").getItem(1)).withColumn("Address_Lane1",split($"address",",").getItem(0)).withColumn("city",split($"address",",").getItem(1)).withColumn("state",split($"address",",").getItem(2)).withColumn("zipcode",split($"address",",").getItem(3)).drop("name").drop("address").show()
}
}
