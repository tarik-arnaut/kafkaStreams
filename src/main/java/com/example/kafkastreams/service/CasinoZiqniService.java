package com.example.kafkastreams.service;

import com.example.kafkastreams.ziqni.CasinoZiqni;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

@Service
public class CasinoZiqniService {

  private final KafkaStreams kafkaStreams;

  public CasinoZiqniService(KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
  }

  public CasinoZiqni getCasinoZiqniById(Long casinoZiqniId) {
    return getStore().get(casinoZiqniId);
  }

  private ReadOnlyKeyValueStore<Long, CasinoZiqni> getStore() {
    return kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(
            "casino.ziqni.store", QueryableStoreTypes.keyValueStore()));
  }
}
