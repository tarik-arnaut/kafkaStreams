package com.example.kafkastreams.topology;

import com.example.kafkastreams.config.StreamConfiguration;
import com.example.kafkastreams.model.CasinoTransaction;
import com.example.kafkastreams.model.CasinoZiqni;
import com.example.kafkastreams.model.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
public class CasinoTransactionTopology {

  public static final String CASINO_TRANSACTION = "casino.transaction";

  public static final String CASINO_ZIQNI = "casino.ziqni";

  @Autowired
  public void buildTopology(StreamsBuilder streamsBuilder) {
    Serde<CasinoTransaction> casinoTransactionSerde = new JsonSerde<>(CasinoTransaction.class);
    Serde<CasinoZiqni> casinoZiqniSerde = new JsonSerde<>(CasinoZiqni.class);

    KStream<Long, CasinoZiqni> casinoZiqniStream =
        streamsBuilder.stream(
                CASINO_TRANSACTION, Consumed.with(Serdes.Long(), casinoTransactionSerde))
            .groupByKey()
            .aggregate(
                CasinoZiqni::new,
                (key, value, aggregate) -> aggregate.process(value),
                Materialized.with(Serdes.Long(), casinoZiqniSerde))
            .toStream();
    casinoZiqniStream.to(CASINO_ZIQNI, Produced.with(Serdes.Long(), casinoZiqniSerde));

    var streamConfiguration = StreamConfiguration.getConfiguration();
    var topology = streamsBuilder.build();
    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfiguration);

    kafkaStreams.start();
  }

}
