import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object BroadCast
{
def main(args:Array[String])
{
 val spark=SparkSession.builder().appName("BroadCastTesting").master("local[5]").getOrCreate();
 val sc=spark.SparkContext();
 val country=Map(("Ind","India"),("USA","UnitedState"),("Aus","Australia"));
 val state=Map(("DEL","Delhi"),("NY","NewYork"),("SYD","Sydney"));
 val data=Seq(("1","Nishant","Ind","DEL"),("2","Brett","USA","NY"),("3","Smith","Aus","SYD"));
 val broadcaststate=sc.broadcast(state);
 val broadcastCountry=sc.broadcast(country);
 val rdd=sc.parallelize(data);
 val columns=("id","Name","country","state");
 val df=rdd.toDF(columns:_*);
 val rdd2=rdd.map(f=>
 {
 val country1=f._3;
 val state1=f._4;
 val fullcountry=broadcastCountry.value.get(country1).get;
 val fullstate=broadcaststate.value.get(state1).get;
 (f._1,f._2,fullcountry,fullstate)
 }
 );
 val df1=df.map(f=>
 {
 val country1=f.getString(2);
 val state1=f.getString(3);
 val fullcountry=broadcastCountry.value.get(country1).get;
 val fullstate=broadcaststate.value.get(state1).get;
 (f.getString(0),f.getString(1),fullcountry,fullstate)
 }
 ).toDF(columns:_*);
 rdd1.collect();
 df1.show();
}
}