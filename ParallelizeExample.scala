import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ParallelizeExample
{
 def main(args:Array[String])
 {
  val spark=SparkSession.builder().appName("RDDParallelExample").master("local[2]").getOrCreate();
  val sc=spark.SparkContext();
  val list=List(1,2,3,4,5,6,7,8);
  val rdd=sc.parallelize(list);
  val rddcollect=rdd.collect();
  println(rdd.getNumPartitions);
  println(rdd.first());
  rddcollect.foreach(println);
 }
}