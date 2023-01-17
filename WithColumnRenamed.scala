import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
imprt org.apache.spark..sql.functions._
import org.apache.spark.sql.types._
Object WithColumnRenamed
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("WithColumnRenamedTest").master("localhost[4]").getOrCreate();

val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
  Row(Row("Michael ","Rose",""),"40288","M",4000),
  Row(Row("Robert ","","Williams"),"42114","M",4000),
  Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
  Row(Row("Jen","Mary","Brown"),"","F",-1)
);
val schema=new StructType().add("name",new StructType().add("firstname",StringType).add("middlename",StringType).add("lastname",StringType)).add("dob",StringType).add("gender",StringType).add("salary",IntegerType);
val rdd=sc.parallelize(data);
val df=spark.createDataFrame(rdd,schema);
df.withColumnRenamed("dob","dateofbirth");//rename column
val schema2=new StructType().add("fname",StringType).add("mname",StringType).add("lname",StringType)
val df2=df.select(col("name").cast(schema2),col("dob"),col("gender"),col("salary"));
val df3=df.withColumn("fname",col("name.firstname")).withColumn("mname",col("name.middlename")).withColumn("lname",col("name.lastname")).drop("name");
val old_column=Seq("dob","gender","salary","fname","mname","lname");
val new_column=Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName");
val column=old_column.zip(new_column).map(f=>col(f._1).as(f._2));
val df4=df3.select(column:_*);
val df5=df3.toDF(new_column:_*);
}
}