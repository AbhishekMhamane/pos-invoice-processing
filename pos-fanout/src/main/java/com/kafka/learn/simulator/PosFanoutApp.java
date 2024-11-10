package com.kafka.learn.simulator;

import com.kafka.learn.schema.MessageKey;
import com.kafka.learn.schema.PosInvoice;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PosFanoutApp {

  private static final Logger logger = LoggerFactory.getLogger(PosFanoutApp.class);

  @Inject
  AppSerdes appSerdes;

  @Inject
  RecordBuilder recordBuilder;

  @ConfigProperty(name = "app.streams.pos-invoice-topic")
  String inputPosTopic;

  @ConfigProperty(name = "app.streams.shipment-topic")
  String outShipmentTopic;

  @ConfigProperty(name = "app.streams.notification-topic")
  String outNotificationTopic;

  @ConfigProperty(name = "app.streams.hadoop-sink-topic")
  String outHadoopSinkTopic;

  /*
  * Builds the Kafka Streams Topology for the POS Fanout App
  */
  @Produces
  public Topology buildTopology() {
    logger.info("Building Topology for POS Fanout App");

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<MessageKey, PosInvoice> KS0 = streamsBuilder.stream(inputPosTopic
            , Consumed.with(appSerdes.getMessageKeySerde(), appSerdes.getPosInvoiceSerde()).withName("PosInvoice-Consumer"));

    KS0.filter((k, v) -> v.getDeliveryType().toString().equalsIgnoreCase(Constants.DELIVERY_TYPE_HOME_DELIVERY))
            .to(outShipmentTopic, Produced.with(appSerdes.getMessageKeySerde(), appSerdes.getPosInvoiceSerde()));

//    KS0.filter((k, v) -> v.getCustomerType().toString().equalsIgnoreCase(Constants.CUSTOMER_TYPE_PRIME))
//            .mapValues(v -> recordBuilder.getNotification(v))
//            .to(outNotificationTopic, Produced.with(appSerdes.getMessageKeySerde(), appSerdes.getNotificationSerde()));

    // Created a reward KeyValueStore
    StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(Constants.REWARD_STATE_STORE_NAME),
            Serdes.String(),
            Serdes.Double()
    );
    streamsBuilder.addStateStore(kvStoreBuilder);

    KS0.filter((k, v) -> v.getCustomerType().toString().equalsIgnoreCase(Constants.CUSTOMER_TYPE_PRIME))
            .transformValues(() -> new RewardStateProcessor(), Constants.REWARD_STATE_STORE_NAME)
            .to(outNotificationTopic, Produced.with(appSerdes.getMessageKeySerde(), appSerdes.getNotificationSerde()));

    KS0.mapValues(v -> recordBuilder.getHadoopRecords(v))
            .flatMapValues(v -> v)
            .to(outHadoopSinkTopic, Produced.with(appSerdes.getMessageKeySerde(), appSerdes.getHadoopRecordSerde()));

    Topology topology = streamsBuilder.build();
    logger.info("Topology description is: {}", topology.describe());

    return topology;
  }

}
