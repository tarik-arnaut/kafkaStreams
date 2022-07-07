package com.example.kafkastreams.topology;

import com.example.kafkastreams.enriched.EnrichedCasinoTransaction;
import com.example.kafkastreams.request.*;
import com.example.kafkastreams.serde.JsonSerde;
import com.example.kafkastreams.ziqni.CasinoZiqni;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Slf4j
public class CasinoTransactionTopology {

  private final NewTopic paymentTableTopic;
  private final NewTopic accountProductTableTopic;
  private final NewTopic userTableTopic;
  private final NewTopic transactionTableTopic;
  private final NewTopic userTopic;
  private final NewTopic transactionTopic;
  private final NewTopic platformProductTopic;
  private final NewTopic paymentTopic;
  private final NewTopic accountProductTopic;

  private final Serde<UserRequest> userRequestSerde = new JsonSerde<>(UserRequest.class);

  private final Serde<CasinoTransactionRequest> casinoTransactionRequestSerde =
      new JsonSerde<>(CasinoTransactionRequest.class);

  private final Serde<PlatformProductRequest> platformProductRequestSerde =
      new JsonSerde<>(PlatformProductRequest.class);

  private final Serde<PaymentRequest> paymentRequestSerde = new JsonSerde<>(PaymentRequest.class);

  private final Serde<AccountsProductRequest> accountProductRequestSerde =
      new JsonSerde<>(AccountsProductRequest.class);

  public CasinoTransactionTopology(
      @Qualifier("paymentTableTopic") NewTopic paymentTableTopic,
      @Qualifier("accountProductTableTopic") NewTopic accountProductTableTopic,
      @Qualifier("userTableTopic") NewTopic userTableTopic,
      @Qualifier("transactionTableTopic") NewTopic transactionTableTopic,
      @Qualifier("userTopic") NewTopic userTopic,
      @Qualifier("transactionTopic") NewTopic transactionTopic,
      @Qualifier("platformProductTopic") NewTopic platformProductTopic,
      @Qualifier("paymentTopic") NewTopic paymentTopic,
      @Qualifier("accountProductTopic") NewTopic accountProductTopic) {
    this.paymentTableTopic = paymentTableTopic;
    this.accountProductTableTopic = accountProductTableTopic;
    this.userTableTopic = userTableTopic;
    this.transactionTableTopic = transactionTableTopic;
    this.userTopic = userTopic;
    this.transactionTopic = transactionTopic;
    this.platformProductTopic = platformProductTopic;
    this.paymentTopic = paymentTopic;
    this.accountProductTopic = accountProductTopic;
  }

  private static EnrichedCasinoTransaction createEnrichedCasino(
      AccountsProductRequest accountsProductRequest,
      EnrichedCasinoTransaction enrichedCasinoTransaction) {
    if (accountsProductRequest != null) {
      enrichedCasinoTransaction.setAccountsProductRequest(accountsProductRequest);
    }
    return enrichedCasinoTransaction;
  }

  private static EnrichedCasinoTransaction createEnrichedCasinoWithUser(
      UserRequest userRequest, EnrichedCasinoTransaction enrichedCasinoTransaction) {
    if (userRequest != null) {
      enrichedCasinoTransaction.setUserRequest(userRequest);
    }
    return enrichedCasinoTransaction;
  }

  public Topology buildTopology() {
    //    KStream<Long, CasinoZiqni> casinoZiqniStream =
    //        streamsBuilder.stream(
    //                        "casino.transaction", Consumed.with(Serdes.Long(),
    // casinoTransactionSerde))
    //                .filter((aLong, casinoTransaction) ->
    // casinoTransaction.getType().equals("Error"))
    //            .groupByKey()
    //            .aggregate(
    //                CasinoZiqni::new,
    //                (key, value, aggregate) -> aggregate.process(value),
    //                Materialized.<Long, CasinoZiqni, KeyValueStore<Bytes, byte[]>>as(
    //                        "casino.ziqni.store")
    //                    .withKeySerde(Serdes.Long())
    //                    .withValueSerde(casinoZiqniSerde))
    //            .toStream();
    //
    //    casinoZiqniStream.to("casino.ziqni", Produced.with(Serdes.Long(), casinoZiqniSerde));

    Serde<CasinoTransactionRequest> casinoTransactionRequestSerde =
        new JsonSerde<>(CasinoTransactionRequest.class);

    Serde<PaymentRequest> paymentRequestSerde = new JsonSerde<>(PaymentRequest.class);

    Serde<AccountsProductRequest> accountsProductRequestSerde =
        new JsonSerde<>(AccountsProductRequest.class);

    Serde<UserRequest> userRequestSerde = new JsonSerde<>(UserRequest.class);

    Serde<EnrichedCasinoTransaction> enrichedCasinoTransactionSerde =
        new JsonSerde<>(EnrichedCasinoTransaction.class);

    Serde<CasinoZiqni> casinoZiqniSerde = new JsonSerde<>(CasinoZiqni.class);

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, AccountsProductRequest> accountsProductRequestStream =
        streamsBuilder.stream(
                accountProductTopic.name(),
                Consumed.with(Serdes.String(), accountProductRequestSerde))
            .filter(
                (key, value) -> value.getType() != null && value.getType().equals("TRANSACTION"));
    accountsProductRequestStream
        .selectKey((key, value) -> value.getDisplayId())
        .to(
            accountProductTableTopic.name(),
            Produced.with(Serdes.String(), accountProductRequestSerde));

    KStream<Long, PaymentRequest> paymentRequestStream =
        streamsBuilder.stream(
            paymentTopic.name(), Consumed.with(Serdes.Long(), paymentRequestSerde));
    paymentRequestStream.to(
        paymentTableTopic.name(), Produced.with(Serdes.Long(), paymentRequestSerde));

    KStream<Long, PlatformProductRequest> platformProductRequestStream =
        streamsBuilder.stream(
            platformProductTopic.name(), Consumed.with(Serdes.Long(), platformProductRequestSerde));

    KStream<Long, CasinoTransactionRequest> transactionRequestStream =
        streamsBuilder.stream(
            transactionTopic.name(), Consumed.with(Serdes.Long(), casinoTransactionRequestSerde));
    transactionRequestStream.to(
        transactionTableTopic.name(), Produced.with(Serdes.Long(), casinoTransactionRequestSerde));

    KStream<String, UserRequest> userRequestStream =
        streamsBuilder.stream(userTopic.name(), Consumed.with(Serdes.String(), userRequestSerde))
            .filter(
                (key, value) ->
                    value.getClubUuid() != null && value.getClubUuid().equals("tenant"));
    userRequestStream
        .selectKey((key, value) -> value.getUuid())
        .to(userTableTopic.name(), Produced.with(Serdes.String(), userRequestSerde));

    KStream<Long, CasinoTransactionRequest> casinoTransactionRequestStream =
        streamsBuilder
            .table(
                transactionTableTopic.name(),
                Consumed.with(Serdes.Long(), casinoTransactionRequestSerde))
            .toStream();

    GlobalKTable<Long, PaymentRequest> paymentTable =
        streamsBuilder.globalTable(
            paymentTableTopic.name(), Consumed.with(Serdes.Long(), paymentRequestSerde));

    GlobalKTable<String, AccountsProductRequest> accountsProductRequestTable =
        streamsBuilder.globalTable(
            accountProductTableTopic.name(),
            Consumed.with(Serdes.String(), accountsProductRequestSerde));

    GlobalKTable<String, UserRequest> userRequestTable =
        streamsBuilder.globalTable(
            userTableTopic.name(), Consumed.with(Serdes.String(), userRequestSerde));

    KStream<Long, EnrichedCasinoTransaction> enrichedCasinoTransactionStream =
        casinoTransactionRequestStream
            .join(
                paymentTable,
                (casinoKey, casinoValue) -> casinoValue.getPaymentId(),
                (casinoTransaction, payment) ->
                    EnrichedCasinoTransaction.builder()
                        .casinoTransactionRequest(casinoTransaction)
                        .paymentRequest(payment)
                        .build())
            .join(
                accountsProductRequestTable,
                (enrichKey, enrichValue) -> enrichValue.getPaymentRequest().getSourceId(),
                (enrichCasino, accountsProduct) ->
                    createEnrichedCasino(accountsProduct, enrichCasino))
            .join(
                userRequestTable,
                (enrichKey, enrichValue) -> enrichValue.getPaymentRequest().getUuid(),
                (enrichCasino, userRequest) ->
                    createEnrichedCasinoWithUser(userRequest, enrichCasino))
            .selectKey((key, value) -> value.getCasinoTransactionRequest().getId())
            .peek(
                (key, value) ->
                    log.info("Key: " + key.toString() + " - " + " Value " + value.toString()));

    return streamsBuilder.build();
  }

  @Bean
  public KafkaStreams kafkaStreams(@Qualifier("props") Properties properties) {
    var topology = this.buildTopology();
    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

    kafkaStreams.cleanUp();
    kafkaStreams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    return kafkaStreams;
  }
}
