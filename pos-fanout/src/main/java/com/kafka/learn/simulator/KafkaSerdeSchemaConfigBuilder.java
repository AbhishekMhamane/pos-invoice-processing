package com.kafka.learn.simulator;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.config.ConfigDef;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
* Builds the configuration for the Kafka Serde Schema
*/
@ApplicationScoped
public class KafkaSerdeSchemaConfigBuilder {

  @ConfigProperty(name = "serde.schema.config.prefixes")
  Optional<List<String>> prefixes;

  Map<String, String> getConfiguration() {
    HashMap<String, String> result = new HashMap<>();
    ConfigDef configDef = AbstractKafkaSchemaSerDeConfig.baseConfigDef();
    Config appConfig = ConfigProvider.getConfig();
    for (String name : configDef.names()) {
      for (String prefix : prefixes.orElse(List.of(""))) {
        Optional<String> value = appConfig
            .getOptionalValue(prefix.length() == 0 ? "" : (prefix + ".") + name, String.class);
        value.ifPresent(v -> result.put(name, v));
      }
    }
    return result;
  }
}
