package Filtering

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, lit, lower, to_date}


class Filter( path: String) {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Filtering")
    .getOrCreate()

  val df = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path)

//-----------------------------------------------------------------------------

  def filter_by_string(include: String = "String", starts_with:String = "String" , ends_with:String = "String", col_name: String): Unit = {

    if (include != "String" && starts_with != "String" && ends_with != "String") {
      df.filter(lower(col(col_name)).rlike(include) && col(col_name).startsWith(starts_with) && col(col_name).endsWith(ends_with)).show()
    }

    else if (include != "String" && starts_with != "String" ) {
      df.filter(lower(col(col_name)).rlike(include) && col(col_name).startsWith(starts_with)).show()
    }

    else if (include != "String" && ends_with != "String" ) {
      df.filter(lower(col(col_name)).rlike(include) && col(col_name).endsWith(ends_with)).show()
    }

    else if (include != "String" ) {
      df.filter(lower(col(col_name)).rlike(include)).show()
    }

    if (!(include != "String") && starts_with != "String" && ends_with != "String") {
      df.filter(col(col_name).startsWith(starts_with) && col(col_name).endsWith(ends_with)).show()
    }

    else if (starts_with != "String") {
      df.filter(col(col_name).startsWith(starts_with)).show()
    }

    else if (ends_with != "String") {
      df.filter(col(col_name).endsWith(ends_with)).show()
    }

  }


//-----------------------------------------------------------------------------
  def filter_by_int(greater_than:Int = 0 , less_than:Int = 0, exact:Int = 0, col_name: String) = {


    if (exact != 0){
      df.filter(df(col_name) === exact).show()}

    else if (greater_than != 0 && less_than != 0) {
      df.filter(df(col_name) >= greater_than && df(col_name) <= less_than).show()
    }

    else if (greater_than != 0){
      df.filter(df(col_name) >= greater_than).show()
    }

    else if (less_than != 0) {
      df.filter(df(col_name) <= less_than).show()
    }
  }


//-----------------------------------------------------------------------------

  def filter_by_date(before:String = "1950-12-12", after:String = "1950-12-12", exact:String = "1950-12-12", col_name: String) = {


    if (exact != "1950-12-12") {
      // For equality, either equalTo or === :
      df.filter(to_date(df(col_name)) === to_date(lit(exact))).show()

    }


    else if (before != "1950-12-12" && after != "1950-12-12") {
      // filter data where the date is less than x and greater than y
      df.filter((to_date(df(col_name)) <= to_date(lit(before))) && (to_date(df(col_name)) >= to_date(lit(after)))).show()
    }

    else if (after != "1950-12-12") {
      // filter data where the date is greater than x
      df.filter(to_date(df(col_name)) >= to_date(lit(after))).show()
    }

    else if (before != "1950-12-12") {
      // filter data where the date is less than x
      df.filter(to_date(df(col_name)) <= to_date(lit(before))).show()
    }


  }

}
