import org.apache.spark.rdd.RDD
imort org.apache.spark.sql.SparkSession

object Partitioning 
{
def main(args:Array[String])
{
val spark=SparkSession.builder().appName("Partition").master("local[4]").getOrCreate();
val sc=spark.SparkContext();
val Partition_rdd=sc.parallelize(Range(0,20),5);
val Partition_rdd1=sc.parallelize(Range(0,20));
val Partition_rddfile=sc.textFile("C:/Users/Nishant Raj/Desktop/Spark/test.txt",10);
val repartition=Partition_rdd1.repartition(6);
val coalesce_partition=Partition_rdd1.repartition(6);
val output1=Partition_rdd.partitions.size
val output2=Partition_rdd2.partitions.size
val output2=Partition_rddfile.partitions.size
             
Partition_rdd.saveAsTextFile("C:/Users/Nishant Raj/Desktop/Spark/Custom_Partition");
Partition_rdd1.saveAsTextFile("C:/Users/Nishant Raj/Desktop/Spark/Default_Partition");
partition_rddfile.saveAsTextFile("C:/Users/Nishant Raj/Desktop/Spark/Custom_File_Partition");
}
}