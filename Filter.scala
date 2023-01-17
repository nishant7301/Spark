import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StringType,ArrayType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.array_contains

object Filter
{
def main(args:Array[String])
{
val spark=SparkSession.builder().master("localhost["x"]").appName("FilterTest").getOrCreate();
  val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
    Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
  )
val rdd=sc.parallelize(arrayStructureData);
val schema = new StructType().add("name",new StructType().add("firstname",StringType).add("middlename",StringType).add("lastname",StringType)).add("languages", ArrayType(StringType)).add("state", StringType).add("gender", StringType)
val df=spark.createDataFrame(rdd,schema);
df.where(df("state")==="OH").show();
df.where("state=='OH'").show();
df.where(df("state")==="OH" && df("gender")==="F").show();
df.filter(array_contains(df("languages"),"Java")).show();
df.filter(df("name.lastname")==='Williams').show()
}
}