import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Example
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("SparkPractice").master("local[1]").getOrCreate();
val sc=spark.SparkContext;
val rdd=sc.parallelize(ArrayList(1,2,3,4,5,6,7,8));
val output1=rdd.collect();
val output2=rdd.getNumPartitions;
val output3=rdd.first();
val output4=rdd.min();
val output4=rdd.max();

}
}