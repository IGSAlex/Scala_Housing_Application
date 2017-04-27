
import com.github.tototoshi.csv._
import scala.io.Source


object Spark_ReadCSV {
   def main(args: Array[String]) {
     
     val reader = CSVReader.open("src/sample.csv")
     var tableList = getTableRecord(reader.allWithHeaders(),"TableName", "F0005")
     println(tableList.size)
     println(tableList);
   }
   
   
   def getTableRecord (source : List[Map[String, String]], targetFieldName:String, targetFieldValue:String): Map[String, String] = {
     var filtered = source.filter(x => x(targetFieldName)==targetFieldValue);
     if (filtered.size == 0){
         throw new Exception("Value ("+targetFieldName+ ":"+targetFieldValue+") cannot be found be the List[Map[String, String]]" )
     }
     return filtered.head; 
   }
   
}