package Streaming

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.{col, from_json}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

  object KafkaConsumerJson {
    def main(arg: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR) // solo loguea los errores

      // levantar Session de SparkSession
      val spark = SparkSession
        .builder()
        .appName("Kafka Consumer Json") // Nombre de la session
        .master("local[2]") // Indico el numero de recursos que usa. * para que use todos los recursos
        .getOrCreate()

      // leer el fichero json en formato kafka y como se inicializa el productor
      val data = spark.readStream
        .format("kafka") // tipo de fuente Kafka
        .option("kafka.bootstrap.servers", "localhost:9092") // levantar el servidor
        .option("subscribe", "practica") // especificar el topic al que se conecta
        .option("startingOffsets", "earliest") // punto de inicion , earliest offset de las particiones
        .load() // cargo los datos
        .selectExpr("CAST(value AS STRING)") // comando para convertir los datos de binario a string

      // creo una tabla- schema
      val tabla = new StructType()
        .add("id", IntegerType)
        .add("first_name", StringType)
        .add("last_name", StringType)
        .add("email", StringType)
        .add("gender", StringType)
        .add("ip_address", StringType)

      // coger los datos de la conversion y que lo introduzca en la tabla-schema
      val persona = data.select(from_json(col("value"), tabla).as ("personal"))
        .select ("personal.*")



      // escribir los datos por consola
      print("Mostrar los datos por consola")

      val query = persona.writeStream
        .format("console") // que lo muestre por consola
        .outputMode("append") // ir adicionando a la tabla datos que vayan llegando
        .start() // inicio de sesion
        .awaitTermination() // termina la session

    }
  }



