package demo
import java.text.{DateFormat, SimpleDateFormat}
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.near
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{to_timestamp, udf}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import org.mongodb.scala.model.geojson._
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model.Filters


object tweets_assignment {

  def convertToDate(spark: SparkSession, dataframe: DataFrame): DataFrame = {

    import spark.implicits._

    // create a user define function to convert the date string to another format
    val change_format = (timeStamp: String) => {

      // define a formatter to convert to "E MMM dd HH:mm:ss Z yyyy"
      val formatter: DateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")

      // convert it to date type
      val formatted_date = formatter.parse(timeStamp)

      // convert it to the new format
      val new_date = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss ZZZZ").format(formatted_date)

      // return the new format
      new_date
    }

    // create the user define function
    val dateUDF = udf(change_format)

    // change the format of the date
    var data_df = dataframe.withColumn("created_at", dateUDF('created_at))

    // convert the date column to timestamp type
    data_df = data_df.withColumn("created_at", to_timestamp($"created_at", "MM/dd/yyyy HH:mm:ss Z"))

    // return the new dataframe
    data_df
  }

  def readDataSet(path: String, spark: SparkSession): DataFrame = {

    // reading thee data into Dataframe
    val data_df = spark
      .read
      .format("json")
      .load(path)

    // return the dataframe
    data_df
  }

  def saveOnMongoDB(spark: SparkSession, sc: SparkContext, dataframe: DataFrame, collection_name: String): Unit = {


    // convert the date column into timestamp type
    val new_data_df = convertToDate(spark, dataframe)

    // set the configuration to write into database
    val writeConfig = WriteConfig(Map("collection" -> s"$collection_name",
      "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

    // save the dataframe in the database
    MongoSpark.save(new_data_df, writeConfig)
  }

  def createIndexes(collection: MongoCollection[org.bson.Document]): Unit = {

    // indexing the geo location
    collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))

    // indexing the datetime
    collection.createIndex(Indexes.ascending("created_at"))


  }

  def findQueries(collection: MongoCollection[org.bson.Document],
                  word: String, radius: Int, longitude: Double,
                  latitude: Double, start_date: Long, end_date: Long): Int = {


    // define a point object using the long and the lat to use it as reference in searching
    val refPoint = Point(Position(longitude, latitude))

    // define a date formatter to convert from epoch to "MM/dd/yyyy HH:mm:ss"
    val epoch_formatter: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

    // parsing the start date
    val start_date_formatted = epoch_formatter.parse(epoch_formatter.format(start_date))

    // parsing the end date
    val end_date_formatted = epoch_formatter.parse(epoch_formatter.format(end_date))

    // get all tweets published within the location and the time date range
    val collections_within_date_and_time = collection.find(
      Filters.and(
        Filters.and(
          Filters.gt("created_at", start_date_formatted),
          Filters.lt("created_at", end_date_formatted)),
        near("coordinates.coordinates", refPoint, radius, 0)))
      .iterator()
      .toList

    // get the number of the word occurrences in that range of time and location
    val word_count = collections_within_date_and_time
      .map(doc => doc
        .get("text")
        .toString)
      .flatMap(doc => doc
        .split("[.{},/!@#$%^&*()_+?<>\" ]"))
      .count(_.toLowerCase() == word.toLowerCase())


    // return the result
    word_count

  }


  def main(args: Array[String]): Unit = {

    // the epoch time should be in milliseconds use this link to get the epoch time https://epoch.now.sh/
    val path = args(0)
    val database_name = args(1)
    val collection_name = args(2)
    val word = args(3)
    val radius = args(4).toInt
    val longitude = args(5).toDouble
    val latitude = args(6).toDouble
    val start_date = args(7).toLong
    val end_date = args(8).toLong

    // define a sparksession and setup the mongodb url to connect on
    val spark = SparkSession
      .builder()
      .appName("tweets")
      .master("local[*]")
      .config("spark.mongodb.input.uri", s"mongodb://127.0.0.1/$database_name.$collection_name")
      .config("spark.mongodb.output.uri", s"mongodb://127.0.0.1/$database_name.$collection_name")
      .getOrCreate()

    // create spark context
    val sc = spark.sparkContext

    // reading data
    val data_df = readDataSet(path, spark)

    // save the dataframe to the database
    saveOnMongoDB(spark, sc, data_df, collection_name)

    // define a mongoclient and set the link to it
    val mongoClient: MongoClient = new MongoClient("localhost", 27017)

    // get the database
    val database = mongoClient.getDatabase(database_name)

    // get the collection
    val collection = database.getCollection(collection_name)

    // create indexes
    createIndexes(collection)

    // get the number of the word occurrences in that range of time and location
    val word_count: Int = findQueries(collection, word, radius.toInt,
      longitude, latitude, start_date, end_date)

    // print the result
    println(s"The word $word accrued $word_count times")
  }

}
