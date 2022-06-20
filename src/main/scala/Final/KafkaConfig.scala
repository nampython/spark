package Final

import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConfig {
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
}
