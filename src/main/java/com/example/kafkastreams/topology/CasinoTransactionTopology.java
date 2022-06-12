package com.example.kafkastreams.topology;

import com.example.kafkastreams.model.CasinoTransaction;
import com.example.kafkastreams.model.CasinoZiqni;
import com.example.kafkastreams.model.JsonSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class CasinoTransactionTopology {

  public static final String CASINO_TRANSACTION = "casino.transaction";

  public static final String CASINO_ZIQNI = "casino.ziqni";

  private final NewTopic casinoTransactionTopic;
  private final NewTopic casinoZiqniTopic;

  public CasinoTransactionTopology(
      @Qualifier("casinoTransactionTopic") NewTopic casinoTransactionTopic,
      @Qualifier("casinoZiqniTopic") NewTopic casinoZiqniTopic) {
    this.casinoTransactionTopic = casinoTransactionTopic;
    this.casinoZiqniTopic = casinoZiqniTopic;
  }

  public static Topology buildTopology() {
    Serde<CasinoTransaction> casinoTransactionSerde = new JsonSerde<>(CasinoTransaction.class);
    Serde<CasinoZiqni> casinoZiqniSerde = new JsonSerde<>(CasinoZiqni.class);

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<Long, CasinoZiqni> casinoZiqniStream =
        streamsBuilder.stream(
                        "casino.transaction", Consumed.with(Serdes.Long(), casinoTransactionSerde))
                .filter(new Predicate<Long, CasinoTransaction>() {
                  @Override
                  public boolean test(Long aLong, CasinoTransaction casinoTransaction) {
                    return casinoTransaction.getType().equals("Error");
                  }
                })
            .groupByKey()
            .aggregate(
                CasinoZiqni::new,
                (key, value, aggregate) -> aggregate.process(value),
                Materialized.<Long, CasinoZiqni, KeyValueStore<Bytes, byte[]>>as(
                        "casino.ziqni.store")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(casinoZiqniSerde))
            .toStream();

    casinoZiqniStream.to("casino.ziqni", Produced.with(Serdes.Long(), casinoZiqniSerde));

    return streamsBuilder.build();
  }
}
