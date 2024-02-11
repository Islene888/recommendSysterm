package com.Sohu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


// 定义电影数据模型，对应MongoDB中的Movie集合，包含电影ID，名称，描述等字段
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

// 定义评分数据模型，对应MongoDB中的Rating集合，包含用户ID，电影ID，评分和时间戳
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

// 定义MongoDB配置的数据模型，包含MongoDB的连接URI和数据库名
case class MongoConfig(uri:String, db:String)

// 定义基础推荐对象的数据模型，包含电影ID和评分
case class Recommendation( mid: Int, score: Double )

// 定义按类别推荐的数据模型，包含类别名和推荐列表（一系列推荐对象）
case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )


// 定义StatisticsRecommender对象，这是整个程序的入口和框架
object StatisticsRecommender {

  // 定义MongoDB中使用的集合名称常量，为了代码的可读性和后期维护方便
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // 定义统计的表的名称常量，便于在MongoDB中存储和查询
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  // 主函数是程序的入口点
  def main(args: Array[String]): Unit = {
    // 使用Map来存储配置参数，便于管理和维护
    val config = Map(
      "spark.cores" -> "local[*]",  // 设置Spark的核心数，local[*]意味着使用所有可用的核心
      "mongo.uri" -> "mongodb://localhost:27017/recommender",  // 设置MongoDB的连接URI
      "mongo.db" -> "recommender"  // 设置MongoDB的数据库名
    )

    // 初始化Spark配置，设置Master URL和应用名
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    // 创建SparkSession对象，是Spark操作的入口点，可以进行数据读取、处理和写入等操作
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 导入隐式转换，这是使用DataFrame和Dataset API的前提
    import spark.implicits._

    // 使用隐式声明的方式，构造MongoConfig实例，后续可以在需要MongoDB配置的地方自动使用
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 从MongoDB中读取评分数据集，使用了MongoDB的Spark连接器
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)  // 设置连接URI
      .option("collection", MONGODB_RATING_COLLECTION)  // 设置读取的集合为Rating
      .format("com.mongodb.spark.sql")  // 设置数据源格式为MongoDB
      .load()  // 加载数据
      .as[Rating]  // 将数据转换为Rating类型的Dataset
      .toDF()  // 将Dataset转换为DataFrame

    // 从MongoDB中读取电影数据集，处理方式类似评分数据集
    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // 创建名为ratings的临时表，方便使用Spark SQL对评分数据进行查询和处理
    ratingDF.createOrReplaceTempView("ratings")

    // 1. 历史热门电影统计
    // 使用Spark SQL执行查询，计算每部电影的评分次数，并命名为count
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    // 调用storeDFInMongoDB函数，将统计结果保存到MongoDB的RateMoreMovies集合中
    storeDFInMongoDB( rateMoreMoviesDF, RATE_MORE_MOVIES )

    // 2. 近期热门电影统计
    // 创建SimpleDateFormat对象，用于将时间戳转换为年月格式，如201906
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF（用户定义函数），命名为changeDate，用于在SQL中直接转换时间戳格式
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt )

    // 对原始评分数据进行预处理，提取电影ID、评分和时间戳转换后的年月信息
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    // 创建名为ratingOfMonth的临时表，方便后续处理
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // 使用Spark SQL执行查询，计算每部电影在每个月的评分次数，并按年月和评分次数进行降序排列
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
    // 调用storeDFInMongoDB函数，将统计结果保存到MongoDB的RateMoreRecentlyMovies集合中
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. 优质电影统计
    // 使用Spark SQL执行查询，计算每部电影的平均评分，并命名为avg
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    // 调用storeDFInMongoDB函数，将统计结果保存到MongoDB的AverageMovies集合中
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. 各类别电影Top统计
    // 定义所有电影类别的列表
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // 把平均评分数据添加到电影详情数据中，通过电影ID连接，使用内连接inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // 将电影类别列表转换为RDD，方便进行笛卡尔积操作
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // 对每个电影类别和电影详情数据进行笛卡尔积操作，生成所有可能的类别与电影的组合
    // 对生成的组合进行过滤，选择那些电影类别字段包含当前类别的电影
    // 将过滤后的数据映射为(类别, (电影ID, 平均评分))的形式
    // 按电影类别进行分组，然后对每个类别的电影按平均评分降序排列，选出前10部
    // 将结果映射为GenresRecommendation对象
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // 过滤条件：电影的类别字段genres包含当前类别genre
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains( genre.toLowerCase )
      }
      .map{
        // 映射操作：转换为(类别, (电影ID, 平均评分))的形式
        case (genre, movieRow) => ( genre, ( movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg") ) )
      }
      .groupByKey()
      // 对每个类别的电影分组
      .map{
        // 对每个类别的电影列表按平均评分降序排列，选出前10部，并转换为GenresRecommendation对象
        case (genre, items) => GenresRecommendation( genre, items.toList.sortWith(_._2>_._2).take(10).map( item=> Recommendation(item._1, item._2)) )
      }
      .toDF()

    // 调用storeDFInMongoDB函数，将统计结果保存到MongoDB的GenresTopMovies集合中
    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    // 关闭SparkSession，释放资源
    spark.stop()
  }

  // 定义storeDFInMongoDB函数，用于将DataFrame数据保存到MongoDB的指定集合中
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)  // 设置MongoDB的连接URI
      .option("collection", collection_name)  // 设置MongoDB的目标集合名
      .mode("overwrite")  // 设置写入模式为覆盖，即如果集合已存在，则覆盖
      .format("com.mongodb.spark.sql")  // 设置数据源格式为MongoDB
      .save()  // 执行保存操作
  }

}


