import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.max


object SkiAnalysis {
  def main(args: Array[String]) {

    val APPNAME = "Ski Data Analysis"

    val OutputLocation = args(0)

    val conf = new SparkConf().setAppName(APPNAME)

    // Use the spark session instead of spark context.
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // The schema is encoded in a string
    val schemaString = "resort_id day_num skier_id lift_id timestamp"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, IntegerType, nullable = false))
    val schema = StructType(fields)

    val skiData = spark.read.format("csv").schema(schema).load("s3://bsds-ski-dx/day6-10.csv")

    //create temp table and then used in SQL statements.
    skiData.createOrReplaceTempView("skiData")

    val totalLiftByDay = spark.sql("SELECT day_num, count(*) AS total_lifts FROM skiData GROUP BY day_num ORDER BY day_num")
    totalLiftByDay.coalesce(1).write.format("csv").option("header", true).save(s"$OutputLocation/count_for_day")

    val countByLiftAndDay = spark.sql("SELECT day_num, lift_id, count(*) AS count FROM skiData GROUP BY day_num, lift_id ORDER BY count")
    val maxCountByDay = countByLiftAndDay.groupBy("day_num").agg(max("count").alias("count")).orderBy("day_num")
    val mostPopularLiftByDay = countByLiftAndDay.join(maxCountByDay, Seq("count", "day_num")).select("day_num", "lift_id", "count").orderBy("day_num")
    mostPopularLiftByDay.coalesce(1).write.format("csv").option("header", true).save(s"$OutputLocation/most_popular_lift_by_day")

    val countByTimeAndDay = spark.sql("SELECT day_num, timestamp, count(*) AS count FROM skiData GROUP BY day_num, timestamp ORDER BY count")
    val maxTimeCountByDay = countByTimeAndDay.groupBy("day_num").agg(max("count").alias("count")).orderBy("day_num")
    val mostPopularTimeByDay = countByTimeAndDay.join(maxTimeCountByDay, Seq("count", "day_num")).select("day_num", "timestamp", "count").orderBy("day_num")
    mostPopularTimeByDay.coalesce(1).write.format("csv").option("header", true).save(s"$OutputLocation/most_popular_time_by_day")

    // Average Vertical per person per Day
    // 1. total vertical of day
    // 2. total skiers of day
    val totalVerticalByDay = spark.sql("SELECT day_num, COUNT(DISTINCT(skier_id)) AS skier_count, " +
      "SUM(CASE " +
      "WHEN lift_id BETWEEN 1 AND 10 THEN 200 " +
      "WHEN lift_id BETWEEN 11 AND 20 THEN 300 " +
      "WHEN lift_id BETWEEN 21 AND 30 THEN 400 " +
      "WHEN lift_id BETWEEN 31 AND 40 THEN 500 " +
      "END) AS total_vertical " +
      "FROM skiData GROUP BY day_num ORDER BY day_num")

    val avgVertPerSkierPerDay = totalVerticalByDay.withColumn("avg", totalVerticalByDay("total_vertical") / totalVerticalByDay("skier_count"))
    avgVertPerSkierPerDay.coalesce(1).write.format("csv").option("header", true).save(s"$OutputLocation/avg_vert_per_skier_per_day")
  }
}
