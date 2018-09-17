package com.netease.music.algo.event.model

import breeze.linalg.DenseVector
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
  * Created by hzrongyuanzhen on 2017/3/7.
  */
object UserFunctions {

  case class HierarchyEventClickrate(eventId: Long, impressCount: Long, clickCount: Long, clickrate: Double, hierarchyTypeName: String, hierarchyType: Int)

  def replaceString(before: String, after: String): UserDefinedFunction = udf((line: String) => {
    line.replace(before, after)
  })

  /**
    * 处理底表中的语种
    * 标准 7 96 16 8
    * {"0":"无","1":"中文","2":"英文","3":"韩语","4":"日语","5":"少数","6":"全量"}
    *
    * @return OneHotEncode 的语种向量
    */
  def transformLanguageEvent: UserDefinedFunction = udf((lang: Int) => {

    // 前四位代表四个语种，最后一位代表缺失语种信息
    val langVector = mutable.ArrayBuffer(0, 0, 0, 0, 0)
    val validLang = Set(1, 2, 3, 4)
    var hasValidLang = false
    if (validLang.contains(lang)) {
      langVector(lang - 1) = 1
      hasValidLang = true
    }
    if (!hasValidLang)
      langVector(4) = 1

    langVector.toArray
  })

  def getOldLanguage(newLan: String): String = {

    if (newLan.equals("1"))
      "7"
    else if (newLan.equals("2"))
      "96"
    else if (newLan.equals("3"))
      "16"
    else if (newLan.equals("4"))
      "8"
    else if (newLan.equals("5"))
      "5"
    else
      "0"
  }

  def getNewLanguage(newLan: String): String = {

    if (newLan.equals("7"))
      "1"
    else if (newLan.equals("96"))
      "2"
    else if (newLan.equals("16"))
      "3"
    else if (newLan.equals("8"))
      "4"
    else
      "0"
  }

  def getNewLanguagePrefs = udf((oldLanguagePrefs: String) => {

    val prefSet: mutable.Set[String] = mutable.Set[String]()
    oldLanguagePrefs.split(",").foreach(line => {
      val info = line.split(":")
      val languageNew = getNewLanguage(info(0))
      if (!languageNew.equals("0"))
        prefSet += (languageNew + ":" + info(1))
    })
    if (prefSet.size > 0)
      prefSet.mkString(",")
    else
      "null"
  })

  def transformLanguageEvent2(langMap: mutable.HashMap[Int, Int]): UserDefinedFunction = udf((lang: Int) => {
    val langVector = mutable.ArrayBuffer.fill[Double](langMap.size + 1)(0)
    if (langMap.contains(lang)) {
      langVector(langMap(lang)) = 1
    } else {
      // 其他值，最后一位置为1
      langVector(langMap.size) = 1
    }
    langVector.toArray
    //val langVectorF = for(i <- 0 until langVector.length) yield langVector(i).toDouble
    // Vectors.dense(langVectorF.toArray)
  })

  def transformSourceTypeEvent(sourceTypeMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((sourceType: String) => {
    val sourceTypeVector = mutable.ArrayBuffer.fill[Double](sourceTypeMap.size + 1)(0)
    if (sourceTypeMap.contains(sourceType)) {
      sourceTypeVector(sourceTypeMap(sourceType)) = 1
    } else {
      // 其他值， 最后一位值1
      sourceTypeVector(sourceTypeMap.size) = 1
    }
    sourceTypeVector.toArray
    //val sourceTypeVectorF = for (i <- 0 until sourceTypeVector.length) yield sourceTypeVector(i).toDouble
    //Vectors.dense(sourceTypeVectorF.toArray)
  })

  def transformLanguageUser: UserDefinedFunction = udf((lang: String) => {

    // 前四位代表四个语种，最后一位代表缺失语种信息
    val langVector = mutable.ArrayBuffer[Double](0, 0, 0, 0, 0)
    var hasValidLang = false
    if (lang != null) {
      val validLang = Map(7 -> 0, 96 -> 1, 16 -> 2, 8 -> 3)
      for (i <- lang.split(",")) {
        val tuple = i.split(":")
        val lang = tuple(0).toInt
        val weight = tuple(1).toDouble
        if (validLang.contains(lang) && weight > 0.1) {
          langVector(validLang(lang)) = weight
          hasValidLang = true
        }
      }
    }
    if (!hasValidLang)
      langVector(4) = 1

    langVector.toArray
  })

  // copy 返回vector, TODO event和user保持一致
  def transformLanguageUser2: UserDefinedFunction = udf((lang: String) => {

    // 前四位代表四个语种，最后一位代表缺失语种信息
    val langVector = mutable.ArrayBuffer[Double](0, 0, 0, 0, 0)
    var hasValidLang = false
    if (lang != null) {
      val validLang = Map(7 -> 0, 96 -> 1, 16 -> 2, 8 -> 3)
      for (i <- lang.split(",")) {
        val tuple = i.split(":")
        val lang = tuple(0).toInt
        val weight = tuple(1).toDouble
        if (validLang.contains(lang) && weight > 0.1) {
          langVector(validLang(lang)) = weight
          hasValidLang = true
        }
      }
    }
    if (!hasValidLang)
      langVector(4) = 1
    langVector.toArray
    //val langVectorF = for(i <- 0 until langVector.length) yield langVector(i).toDouble
    //Vectors.dense(langVectorF.toArray)

  })

  def transformLanguageUserInEvent = udf((lang: String) => {

    // 前四位代表四个语种，最后一位代表缺失语种信息
    val langVector = mutable.ArrayBuffer[Double](0, 0, 0, 0, 0)
    val validLang = Set(1, 2, 3, 4)
    var hasValidLang = false
    if (lang != null) {
      for (i <- lang.split(",")) {
        val tuple = i.split(":")
        val lang = tuple(0).toInt
        val weight = tuple(1).toDouble
        if (validLang.contains(lang)) {
          langVector(lang - 1) = 1
          hasValidLang = true
        }
      }
    }
    if (!hasValidLang)
      langVector(4) = 1

    langVector.toArray
  })

  /**
    * 将动态的分类做相应的oneHotEncode
    *
    * @param categoryMap 包含所有分类的配置文件
    * @return 二级分类做oneHotEncode的向量
    */
  def transformCategoryEvent(categoryMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((category: String) => {

    val categoryVector = mutable.ArrayBuffer.fill[Int](categoryMap.size)(0)

    if (categoryMap.contains(category))
      categoryVector(categoryMap(category)) = 1

    categoryVector.toArray

  })

  def transformCategoryEvent2(categoryMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((category: String) => {
    val categoryVector = mutable.ArrayBuffer.fill[Double](categoryMap.size + 1)(0)
    //for (i <- category) {
    // 二级类目
    // val cate = category.replace(",", "_")
    if (categoryMap.contains(category)) {
      categoryVector(categoryMap(category)) = 1
    }
    // 一级类目
    val topCat = category.split("_")(0)
    if (categoryMap.contains(topCat)) {
      categoryVector(categoryMap(topCat)) = 1
    } else {
      categoryVector(categoryMap.size) = 1
    }
    //}
    categoryVector.toArray
    //val categoryVectorF = for (i <- 0 until categoryVector.length) yield categoryVector(i).toDouble
    //Vectors.dense(categoryVectorF.toArray)
  })

  def transformCategoryUser(categoryMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((category: String) => {

    val categoryVector = mutable.ArrayBuffer.fill[Double](categoryMap.size)(0)

    if (category != null) {
      for (i <- category.split(",")) {
        val tuple = i.split(":")
        val cate = tuple(0)
        // 分数中存了一部分NaN
        val score = if (tuple(1).toDouble.isNaN) 0.0 else tuple(1).toDouble
        if (categoryMap.contains(cate))
          categoryVector(categoryMap(cate)) = score
      }
    }

    categoryVector.toArray

  })

  // TODO, event 和 user 统一格式
  def transformCategoryUser2(categoryMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((category: String) => {

    val categoryVector = mutable.ArrayBuffer.fill[Double](categoryMap.size + 1)(0)
    if (category != null) {
      for (i <- category.split(",")) {
        val tuple = i.split(":")
        val cate = tuple(0)
        // 二级分类
        // 分数中存了一部分NaN
        val score = if (tuple(1).toDouble.isNaN) 0.0 else tuple(1).toDouble
        if (categoryMap.contains(cate)) {
          categoryVector(categoryMap(cate)) = score
        }
        // 上一级分类
        val topCat = cate.split("_")(0)
        if (categoryMap.contains(topCat)) {
          categoryVector(categoryMap(topCat)) = score
        } else {
          categoryVector(categoryMap.size) = score
        }
      }
    }
    categoryVector.toArray
    //val categoryVectorF = for(i <- 0 until categoryVector.length) yield categoryVector(i).toDouble
    //Vectors.dense(categoryVectorF.toArray)
  })

  def transformAlg: UserDefinedFunction = udf((alg: String) => {

    val summarizedAlgArray = Array("hotEvent", "category", "goodEvent", "keyword",
      "itembased", "tag", "artist", "song", "creator", "other")

    val summarizedAlg = alg match {
      case _ if alg.contains("hotEvent") => summarizedAlgArray(0)
      case _ if alg.startsWith("category") => summarizedAlgArray(1)
      case _ if alg.startsWith("goodEvent") => summarizedAlgArray(2)
      case _ if alg.contains("keyword") => summarizedAlgArray(3)
      case _ if alg.contains("itembased") => summarizedAlgArray(4)
      case _ if alg.startsWith("tag_E") => summarizedAlgArray(5)
      case _ if alg.toLowerCase.contains("artist") => summarizedAlgArray(6)
      case _ if alg.startsWith("playaction") => summarizedAlgArray(7)
      case "userEvent" => summarizedAlgArray(8)
      case _ => summarizedAlgArray(9)
    }

    val algVector = mutable.ArrayBuffer.fill[Int](summarizedAlgArray.size)(0)

    algVector(summarizedAlgArray.indexOf(summarizedAlg)) = 1

    algVector.toArray

  })

  def transformAlg2(algMap: mutable.HashMap[String, Int]): UserDefinedFunction = udf((alg: String) => {
    val algVector = mutable.ArrayBuffer.fill[Double](algMap.size + 1)(0)
    val prefixAlg = alg.split("_")(0)
    if (algMap.contains(prefixAlg)) {
      algVector(algMap(prefixAlg)) = 1
    } else {
      algVector(algMap.size) = 1
    }
    algVector.toArray
    //val algVectorF = for(i <- 0 until algVector.length) yield algVector(i).toDouble
    //Vectors.dense(algVectorF.toArray)
  })

  def transToAlgvector: UserDefinedFunction = udf((alg: Int) => {

    val summarizedAlgArray = Array("hotEvent", "category", "goodEvent", "keyword",
      "itembased", "tag", "artist", "song", "creator", "other")

    val summarizedAlg = alg match {
      case _ if alg == 6 => summarizedAlgArray(0)
      case _ if alg == -1 => summarizedAlgArray(1)
      case _ if alg == 5 => summarizedAlgArray(2)
      case _ if alg == 11 => summarizedAlgArray(3)
      case _ if alg == 4 => summarizedAlgArray(4)
      case _ if alg == -12 => summarizedAlgArray(5)
      case _ if alg == -2 => summarizedAlgArray(6)
      case _ if alg == 2 => summarizedAlgArray(7)
      case _ if alg == -10 => summarizedAlgArray(8)
      case _ => summarizedAlgArray(9)
    }

    val algVector = mutable.ArrayBuffer.fill[Double](summarizedAlgArray.size)(0)

    algVector(summarizedAlgArray.indexOf(summarizedAlg)) = 1

    Vectors.dense(algVector.toArray)

  })

  def computeSmoothRate(alpha: Int, beta: Int): UserDefinedFunction = udf((impressCount: Long, clickCount: Long) => {

    (clickCount.toDouble + alpha) / (impressCount + beta)

  })

  def containsArtist: UserDefinedFunction = udf((eventArtistId: Long, artistSet: Seq[Long]) => {

    if (artistSet != null && eventArtistId > 0) {
      if (artistSet.contains(eventArtistId)) 1 else 0
    } else
      0

  })

  //  def containsArtist:UserDefinedFunction = udf((eventArtistId:Seq[Long], artistSet:Seq[Long]) => {
  //
  //    if (artistSet != null && eventArtistId != null) {
  //      val intersection = eventArtistId.intersect(artistSet)
  //      if (intersection.nonEmpty) 1 else 0
  //    } else
  //      0
  //
  //  })

  def multiplyVectors(size: Int): UserDefinedFunction = udf((eventFeatureVector: Seq[Int], userFeatureVector: Seq[Double]) => {

    if (eventFeatureVector != null && userFeatureVector != null) {
      val bv1 = new DenseVector(eventFeatureVector.map(_.toDouble).toArray)
      val bv2 = new DenseVector(userFeatureVector.toArray)
      Vectors.dense((bv1 :* bv2).toArray)
    } else {
      Vectors.dense(Array.fill[Double](size)(0.0))
    }
  })

  def fillNAFunction[T](input: Seq[T])(implicit n: Numeric[T]) = {

    val size = input.size

    if (input != null) {
      Vectors.dense(input.map(num => n.toDouble(num)).toArray)
    } else {
      Vectors.dense(Array.fill[Double](size)(0.0))
    }
  }

  /**
    * 如果这个特征向量为null，填充一个全为0，长度为size的向量
    *
    * @param size 被填充的向量长度
    * @return
    */
  def fillNAVectors(size: Int): UserDefinedFunction = udf((input: Seq[java.lang.Number]) => {

    if (input != null) {
      Vectors.dense(input.map(_.doubleValue()).toArray)
    } else {
      Vectors.dense(Array.fill[Double](size)(0.0))
    }
  })

  def fillNAVectors2(size: Int): UserDefinedFunction = udf((input: Seq[Double]) => {

    if (input != null) {
      Vectors.dense(input.toArray)
    } else {
      Vectors.dense(Array.fill[Double](size)(0.0))
    }
  })

  /**
    * 如果这个特征向量为null，填充一个全为initValue，长度为size的向量
    *
    * @param size
    * @param initValue
    * @return
    */
  def fillNAVectors(size: Int, initValue: Double): UserDefinedFunction = udf((input: Seq[Int]) => {

    if (input != null) {
      Vectors.dense(input.map(_.toDouble).toArray)
    } else {
      Vectors.dense(Array.fill[Double](size)(initValue))
    }
  })

  /**
    *
    * @param size
    * @param initValue
    * @param fromVector
    * @return
    */
  def fillNAVectors(size: Int, initValue: Double, fromVector: Boolean): UserDefinedFunction = udf((input: ml.linalg.DenseVector) => {

    if (input != null) {
      input
    } else {
      Vectors.dense(Array.fill[Double](size)(initValue))
    }
  })

  def log10Int: UserDefinedFunction = udf((score: Long) => {

    val vector = mutable.ArrayBuffer.fill[Double](5)(0)

    if (score == 0)
      vector(0) = 1
    else if (score > 0 && score < 100000) {
      vector(math.log10(score).toInt) = 1
    } else {
      vector(4) = 1
    }

    Vectors.dense(vector.toArray)

  })

  def log1p: UserDefinedFunction = udf((score: Long) => math.log1p(score))

  def getUserRefreshType: UserDefinedFunction = udf((refresh_cnt_adjusted: Float, rcmd_cnt: Integer) => {

    if (refresh_cnt_adjusted > 3 && rcmd_cnt > 20)
      3
    else if (refresh_cnt_adjusted > 0 && rcmd_cnt > 9) {
      2
    } else {
      1
    }
  })

  def hierarchyEventClickrateUDF: UserDefinedFunction = udf((eventId: Long, impressCount: Long, clickCount: Long, clickrate: Double, hierarchyTypeName: String, hierarchyType: Int) =>
    HierarchyEventClickrate(eventId, impressCount, clickCount, clickrate, hierarchyTypeName, hierarchyType)
  )

  def getUserLanguageMap: UserDefinedFunction = udf((eventLanguage: String) => {

    val info = eventLanguage.split(",")
    for (item <- info) {
      val info2 = item.split(":")
      val lan = info2(0).toLong
      val pref = info2(1).toFloat


    }
  })






  def computeEncodedFeature: UserDefinedFunction = udf((feature: Row) => {

      if (feature != null && !(feature.isNullAt(1) && feature.isNullAt(2))) {
        val impressCount = feature.getLong(1)
        val clickCount = feature.getLong(2)
        val clickrate = (clickCount.asInstanceOf[Long] + 100).toDouble / (impressCount.asInstanceOf[Long] + 10000)
        impressCount.toString.concat("\t").concat(clickCount.toString).concat("\t").concat(clickrate.toString)
      }
      else "0\t0\t0"
    })


  def computeEncodedFeature1: UserDefinedFunction = udf((feature: Seq[Int]) => {
    feature.mkString("\t")
  })


  /** 根据用户的年龄数据分配相应点击率 */
  def computeClickRateForUserAge: UserDefinedFunction = udf(
    (userAgeType: Int, userAgeClickRate0: Row, userAgeClickRate1: Row, userAgeClickRate2: Row, userAgeClickRate3: Row) => {

      val userAgeClickRateArray = Array(userAgeClickRate0, userAgeClickRate1, userAgeClickRate2, userAgeClickRate3)

      val ageClickRateVector = mutable.ArrayBuffer.fill[Double](4)(0)

      val userAgeClikeRate = userAgeClickRateArray(userAgeType)

      val smoothClickRate = if (userAgeClikeRate != null) {

        if (!(userAgeClikeRate.isNullAt(1) || userAgeClikeRate.isNullAt(2))) {
          val impressCount = userAgeClikeRate.getLong(1)
          val clickCount = userAgeClikeRate.getLong(2)
          (clickCount.asInstanceOf[Long] + 100).toDouble / (impressCount.asInstanceOf[Long] + 1000)
        } else {
          0.1
        }
      } else {
        0.1
      }

      ageClickRateVector(userAgeType) = smoothClickRate

      ageClickRateVector.toArray

    })

  /** event年龄数据分配相应点击率 */
  def computeClickRateForUserAge2: UserDefinedFunction = udf(
    (userAgeClickRate0: Row, userAgeClickRate1: Row, userAgeClickRate2: Row, userAgeClickRate3: Row) => {

      val userAgeClickRateArray = Array(userAgeClickRate0, userAgeClickRate1, userAgeClickRate2, userAgeClickRate3)

      val ageClickRateVector = mutable.ArrayBuffer.fill[Double](4)(0)
      for (i <- 0 until 3) {
        val userAgeClikeRate = userAgeClickRateArray(i)
        val smoothClickRate = if (userAgeClikeRate != null) {
          if (!(userAgeClikeRate.isNullAt(1) || userAgeClikeRate.isNullAt(2))) {
            val impressCount = userAgeClikeRate.getLong(1)
            val clickCount = userAgeClikeRate.getLong(2)
            (clickCount.asInstanceOf[Long] + 100).toDouble / (impressCount.asInstanceOf[Long] + 1000)
          } else {
            0.1
          }
        } else {
          0.1
        }
        ageClickRateVector(i) = smoothClickRate
      }
      ageClickRateVector.toArray
    })

  /**
    * 根据用户的性别分配相应的点击率
    * 用户性别表中还有1和2之外的数据，和0一起理解为未知
    */
  def computeClickRateForUserGender: UserDefinedFunction = udf(
    (userGender: Int, userGenderClickRate0: Row, userGenderClickRate1: Row, userGenderClickRate2: Row) => {

      val userGenderClickRateArray = Array(userGenderClickRate0, userGenderClickRate1, userGenderClickRate2)

      val genderClickRateVector = mutable.ArrayBuffer.fill[Double](3)(0)

      val validGenderSet = Set(1, 2)

      // 不是1和2的值都作为未知，赋0
      val realGender = if (validGenderSet.contains(userGender)) userGender else 0

      val userGenderClikeRate = userGenderClickRateArray(realGender)

      val smoothClickRate = if (userGenderClikeRate != null) {

        if (!(userGenderClikeRate.isNullAt(1) || userGenderClikeRate.isNullAt(2))) {
          val impressCount = userGenderClikeRate.getLong(1)
          val clickCount = userGenderClikeRate.getLong(2)
          (clickCount.asInstanceOf[Long] + 100).toDouble / (impressCount.asInstanceOf[Long] + 1000)
        } else {
          0.1
        }
      } else {
        0.1
      }

      genderClickRateVector(realGender) = smoothClickRate

      genderClickRateVector.toArray

    })

  /**
    * event用户性别点击率
    * 用户性别表中还有1和2之外的数据，和0一起理解为未知
    */
  def computeClickRateForUserGender2: UserDefinedFunction = udf(
    (userGenderClickRate0: Row, userGenderClickRate1: Row, userGenderClickRate2: Row) => {

      val userGenderClickRateArray = Array(userGenderClickRate0, userGenderClickRate1, userGenderClickRate2)

      val genderClickRateVector = mutable.ArrayBuffer.fill[Double](3)(0)

      val validGenderSet = Set(1, 2)
      for (i <- 0 until 2) {
        // 不是1和2的值都作为未知，赋0
        val realGender = if (validGenderSet.contains(i)) i else 0

        val userGenderClikeRate = userGenderClickRateArray(realGender)

        val smoothClickRate = if (userGenderClikeRate != null) {

          if (!(userGenderClikeRate.isNullAt(1) || userGenderClikeRate.isNullAt(2))) {
            val impressCount = userGenderClikeRate.getLong(1)
            val clickCount = userGenderClikeRate.getLong(2)
            (clickCount.asInstanceOf[Long] + 100).toDouble / (impressCount.asInstanceOf[Long] + 1000)
          } else {
            0.1
          }
        } else {
          0.1
        }

        genderClickRateVector(realGender) = smoothClickRate
      }
      genderClickRateVector.toArray

    })

  def getHierarchyEventClickrateFeature(eventId: Long, hierarchyEventclickrate: Iterable[(Long, Long, Long, Double, String, Int)], hierarchyType: String, hierarchyTypeValues: Seq[Int]): Row = {

    val hierarchyTypeSize = hierarchyTypeValues.length
    val hierarchyTypeValuesWithIndexs = hierarchyTypeValues.sorted.zipWithIndex.toMap
    val eventClickrateList = hierarchyEventclickrate.toArray
    val eventClickRates = new Array[Row](hierarchyTypeSize)
    for (i <- 0 to eventClickRates.length - 1) {
      eventClickRates(i) = Row(eventId, 0l, 0l, 0.0, hierarchyType, i)
    }

    for (eventClickrate <- eventClickrateList) {
      val refreshTypeValue = eventClickrate._6
      val index = hierarchyTypeValuesWithIndexs(refreshTypeValue)
      eventClickRates(index) = Row(eventClickrate._1, eventClickrate._2, eventClickrate._3, eventClickrate._4, eventClickrate._5, eventClickrate._6)
    }

    val rowsReturn = for (i <- 0 to hierarchyTypeSize) yield {
      if (i == 0)
        eventId
      else
        eventClickRates(i - 1)
    }
    //    Row(eventId, eventClickRates(0), eventClickRates(1), eventClickRates(2))
    Row.fromSeq(rowsReturn)
  }

  def splitEvents: UserDefinedFunction = udf((events: String) => {
    events.split(",")
  })

  def extractEventId(idPos: Int): UserDefinedFunction = udf((eventInfo: String) => {
    eventInfo.split(":")(idPos).toLong
  })

  def extractAlg(algPos: Int): UserDefinedFunction = udf((eventInfo: String) => {
    eventInfo.split(":")(algPos)
  })


  def getCategoryAlg: UserDefinedFunction = udf((userId: Long) => {
    -1
  })

  def main(args: Array[String]): Unit = {
    println(Seq("1","2","3").mkString(","))
  }
}
