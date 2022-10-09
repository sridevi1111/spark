package sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object sparkobj {
  
 def main(args:Array[String]):Unit={
    val Conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
  
   val df = spark.read.format("json").option("multiline","true")
                                     .load("file:///C:/data/randomuser.json")
    df.show()
    df.printSchema()
    
 val explodedf = df.withColumn("results",explode(col("results")))
    explodedf.printSchema()
    val flattendf = explodedf.select(
                                     col("nationality"),


                                     col("results.user.cell"),
                                     col("results.user.dob"),
                                     col("results.user.email"),
                                     col("results.user.gender"),
                                    col("results.user.location.*"),
                                     col("results.user.md5"),
                                    col( "results.user.name.*"),
                                     col("results.user.password"),
                                    col( "results.user.phone"),
                                     col("results.user.picture.*"),
                                     col("results.user.registered"),
                                     col("results.user.salt"),
                                     col("results.user.sha1"),
                                     col("results.user.sha256"),
                                     col("results.user.username"),
                                     col("seed"),
                                     col("version")
                                     )
                                     
                             flattendf.printSchema()
val structdf = flattendf.select(
                                    col("nationality"),
                                    col("seed"),
                                    col("version"),
                                   
                                    
                                   
                                    
                                    
                                    
                         struct(
                              col("city"),
                              col("state"),
                              col("street"),
                              col("zip")))
                              .alias("location")
                              
                      
                              
                   structdf.printSchema()          
                                    
                                    
 
                             
                             
      
}}