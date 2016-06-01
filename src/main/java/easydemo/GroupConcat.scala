package easydemo

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType

class GroupConcat(val separator: String) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(StructField("value", StringType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("total", StringType) :: Nil)

  def dataType: DataType = StringType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer.isNullAt(0)){
      buffer(0) = input.getAs[String](0)
    } else {
      buffer(0) = buffer.getAs[String](0).concat(separator).concat(input.getAs[String](0))
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.isNullAt(0)){
      buffer1(0) = buffer2.getAs[String](0)
    } else {
      buffer1(0) = buffer1.getAs[String](0).concat(separator).concat(buffer2.getAs[String](0))
    }
  }

  def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }
}
