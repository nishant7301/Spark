import implicits._*
import org.apache.spark.sql.SparkSession

object SelectExample
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("SelectExample").master("local[4]").getOrCreate();
val sc=spark.SparkContext;
val data = Seq(("James","Smith","USA","CA"),
  ("Michael","Rose","USA","NY"),
  ("Robert","Williams","USA","CA"),
  ("Maria","Jones","USA","FL")
  );
val columns = Seq("firstname","lastname","country","state")

val df=data.toDF(columns:_*);
val dfselect=df.select("*").show();
val dftwocolumn=df.select("firstname","country").show();

}
}
