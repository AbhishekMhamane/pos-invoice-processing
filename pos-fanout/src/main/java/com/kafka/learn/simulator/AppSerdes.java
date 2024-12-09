package com.kafka.learn.simulator;

import com.kafka.learn.schema.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;

/*
* Builds the serdes for the Kafka Streams
*/
@ApplicationScoped
public class AppSerdes {

  @Inject
  KafkaSerdeSchemaConfigBuilder kafkaSerdeSchemaConfigBuilder;

  public Serde<MessageKey> getMessageKeySerde() {
    Serde<MessageKey> serde = new SpecificAvroSerde<>();
    serde.configure(kafkaSerdeSchemaConfigBuilder.getConfiguration(), true);
    return serde;
  }

  public Serde<PosInvoice> getPosInvoiceSerde() {
    Serde<PosInvoice> serde = new SpecificAvroSerde<>();
    serde.configure(kafkaSerdeSchemaConfigBuilder.getConfiguration(), false);
    return serde;
  }

  public Serde<Notification> getNotificationSerde() {
    Serde<Notification> serde = new SpecificAvroSerde<>();
    serde.configure(kafkaSerdeSchemaConfigBuilder.getConfiguration(), false);
    return serde;
  }

  public Serde<HadoopRecord> getHadoopRecordSerde() {
    Serde<HadoopRecord> serde = new SpecificAvroSerde<>();
    serde.configure(kafkaSerdeSchemaConfigBuilder.getConfiguration(), false);
    return serde;
  }

  public Serde<Store> getStoreSerde() {
    Serde<Store> serde = new SpecificAvroSerde<>();
    serde.configure(kafkaSerdeSchemaConfigBuilder.getConfiguration(), false);
    return serde;
  }

}
