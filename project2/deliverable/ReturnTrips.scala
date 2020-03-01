import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._

    val earthRadius = 6371000
    val dpDist = dist
    val dt = 8
    // determine longitude diff for dist meters at lat
    val longitudeDiff = (lat: Double, dist : Double) => 360D * dist / (2 * math.Pi * earthRadius * math.cos(math.toRadians(lat)))
    val latDiff = (dist : Double) => dist * 360D / (earthRadius * 2 * math.Pi)

    val maxLat = trips.agg(max(abs($"pickup_latitude"))).as[Double].first() + 0.01
    // val maxLat = 61
    val bucketWidthLat = latDiff(dpDist)
    val bucketWidthLon = longitudeDiff(maxLat, dpDist)
    //println("bucketWidtLat: " + bucketWidthLat)
    //println("bucketWidtLon: " + bucketWidthLon)

    val makeDistExpr = (lat1 : Column, lon1 : Column, lat2 : Column, lon2 : Column) => {
        val dLat = toRadians(abs(lat2 - lat1))
        val dLon = toRadians(abs(lon2 - lon1))
        val hav = pow(sin(dLat*0.5),2) + pow(sin(dLon*0.5),2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
        abs(lit(earthRadius * 2) * asin(sqrt(hav)))
    }

    val cellsLat = math.ceil(180 / bucketWidthLat).toLong
    val cellsLon = math.ceil(360 / bucketWidthLon).toLong
    val makeLatBucketExpr = (lat : Column) => {
      floor(lat * lit(1/bucketWidthLat))
    }
    val makeLonBucketExpr = (lon : Column) => {
      floor(lon * lit(1/bucketWidthLon))
    }
    val makeTimeBucketExpr = (time : Column) => {
      floor(time * lit(1.0/(dt * 60 * 60)))
    }

    val returnsForTrips = (trips : Dataset[Row], returns : Dataset[Row]) => {
      val to = trips.as("to")
      val back = returns
        .drop($"tpep_dropoff_datetime")
        .drop($"dropTimeBucket")
        .drop($"lonBucketDropoff")
        .drop($"latBucketDropoff")
        .withColumn("latBucketPickup", explode(array(
        $"latBucketPickup" - 1,
        $"latBucketPickup",
        $"latBucketPickup" + 1)))
        .withColumn("lonBucketPickup", explode(array(
        $"lonBucketPickup" - 1,
        $"lonBucketPickup",
        $"lonBucketPickup" + 1)))
        .withColumn("pickupTimeBucket", explode(array(
        $"pickupTimeBucket",
        $"pickupTimeBucket" - 1)))
        .as("back")
      to.join(back,
        $"to.dropTimeBucket" === $"back.pickupTimeBucket" &&
        $"to.latBucketDropoff" === $"back.latBucketPickup" &&
        $"to.lonBucketDropoff" === $"back.lonBucketPickup"
        )
        .filter(
            $"to.tpep_dropoff_datetime" < $"back.tpep_pickup_datetime" &&
            $"back.tpep_pickup_datetime" < $"to.tpep_dropoff_datetime" + lit(dt * 60 * 60) &&
            abs($"to.pickup_longitude" - $"back.dropoff_longitude") < lit(bucketWidthLon) &&
            abs($"to.pickup_latitude" - $"back.dropoff_latitude") < lit(bucketWidthLat) &&
            abs($"back.pickup_longitude" - $"to.dropoff_longitude") < lit(bucketWidthLon) &&
            abs($"back.pickup_latitude" - $"to.dropoff_latitude") < lit(bucketWidthLat) &&
            makeDistExpr($"back.dropoff_latitude", $"back.dropoff_longitude", $"to.pickup_latitude", $"to.pickup_longitude") < lit(dist) &&
            makeDistExpr($"to.dropoff_latitude", $"to.dropoff_longitude", $"back.pickup_latitude", $"back.pickup_longitude") < lit(dist)
        )
    }

    val bucketize = (trips : Dataset[Row]) => {
      trips
        .select(
        "dropoff_latitude",
        "pickup_latitude",
        "dropoff_longitude",
        "pickup_longitude",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime")
        .withColumn("tpep_pickup_datetime", $"tpep_pickup_datetime".cast(DataTypes.LongType))
        .withColumn("tpep_dropoff_datetime", $"tpep_dropoff_datetime".cast(DataTypes.LongType))
        .withColumn("latBucketDropoff", makeLatBucketExpr($"dropoff_latitude"))
        .withColumn("lonBucketDropoff", makeLonBucketExpr($"dropoff_longitude"))
        .withColumn("latBucketPickup", makeLatBucketExpr($"pickup_latitude"))
        .withColumn("lonBucketPickup", makeLonBucketExpr($"pickup_longitude"))
        .withColumn("dropTimeBucket", makeTimeBucketExpr($"tpep_dropoff_datetime"))
        .withColumn("pickupTimeBucket", makeTimeBucketExpr($"tpep_pickup_datetime"))
    }

    val toBack = (trips : Dataset[Row]) => {
      var bucketized = bucketize(trips).cache()
      returnsForTrips(bucketized, bucketized)
    }
    toBack(trips)
  }
}

