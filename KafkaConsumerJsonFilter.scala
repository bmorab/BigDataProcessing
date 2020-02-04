package Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object KafkaConsumerJsonFilter{
  def main(arg: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // levantar Session de SparkSession
    val spark = SparkSession
      .builder()
      .appName("Kafka Consumer Json")
      .master("local[2]")
      .getOrCreate()

    // leer el fichero json en formato kafka y como se inicializa el productor
    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "practica")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)") // comando para convertir los datos de binario a string

    // creo una tabla
    val tabla = new StructType()
      .add("id", IntegerType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("email", StringType)
      .add("gender", StringType)
      .add("ip_address", StringType)

    // coger los datos de la conversion y que lo introduzca en la tabla
     val persona = data.select(from_json(col("value"), tabla).as ("personal"))
      .select ("personal.*").filter("personal.last_name != 'Bea'" )
       .filter("personal.first_name != 'Willard'")


    // escribir los datos por consola
    print("Mostrar los datos por consola")

    val query = persona.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}



