package Filtering

object Filter_test {
  def main(args:Array[String]): Unit = {

    val path1 = "D:\\Documentations\\Datasets\\flight_summary.csv"
    val path2 = "D:\\Documentations\\Datasets\\aapl.csv"


    val x = new Filter(path1)
    val y = new Filter(path2)

//    x.filter_by_string(include="uni" , col_name = "DEST_COUNTRY_NAME")
//    x.filter_by_int(exact=60, col_name = "count")

//    y.filter_by_date(exact= "2008-10-14", col_name = "Date")

  }
}


