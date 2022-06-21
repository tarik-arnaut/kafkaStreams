package com.example.kafkastreams.topology;

import com.example.kafkastreams.enriched.EnrichedCasinoTransaction;
import com.example.kafkastreams.request.AccountsProductRequest;
import com.example.kafkastreams.request.CasinoTransactionRequest;
import com.example.kafkastreams.request.PaymentRequest;
import com.example.kafkastreams.request.UserRequest;
import com.example.kafkastreams.serde.JsonSerde;
import com.example.kafkastreams.ziqni.CasinoZiqni;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.User;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
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

    //-----------> STREAMS

    KStream<Long, CasinoTransactionRequest> casinoTransactionRequestStream =
        streamsBuilder.stream(
            "casino.transaction", Consumed.with(Serdes.Long(), casinoTransactionRequestSerde));

    KStream<Long, PaymentRequest> paymentRequestStream =
            streamsBuilder.stream(
                    "payment.topic", Consumed.with(Serdes.Long(), paymentRequestSerde));
    paymentRequestStream.to("payment.table.topic", Produced.with(Serdes.Long(), paymentRequestSerde));

    KStream<String, AccountsProductRequest> accountsProductRequestStream =
            streamsBuilder.stream(
                    "accounts.product.topic", Consumed.with(Serdes.String(), accountsProductRequestSerde));
    accountsProductRequestStream.selectKey((key,value)->value.getDisplayId()).to("account.product.table.topic", Produced.with(Serdes.String(), accountsProductRequestSerde));

    KStream<String, UserRequest> userRequestStream =
            streamsBuilder.stream(
                    "user.topic", Consumed.with(Serdes.String(), userRequestSerde));
    userRequestStream.selectKey((key,value)->value.getUuid()).to("user.table.topic", Produced.with(Serdes.String(), userRequestSerde));

    //-----------> STREAMS

    //-----------> TABLES

    GlobalKTable<Long, PaymentRequest> paymentTable =
        streamsBuilder.globalTable(
            "payment.table.topic", Consumed.with(Serdes.Long(), paymentRequestSerde));

    GlobalKTable<String, AccountsProductRequest> accountsProductRequestTable =
        streamsBuilder.globalTable(
            "account.product.table.topic", Consumed.with(Serdes.String(), accountsProductRequestSerde));

    GlobalKTable<String, UserRequest> userRequestTable =
        streamsBuilder.globalTable("user.table.topic", Consumed.with(Serdes.String(), userRequestSerde));

    //-----------> TABLES

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

    //
    //    ValueJoiner<CasinoTransactionRequest, PaymentRequest, EnrichedCasinoTransaction>
    //        casinoPaymentJoiner =
    //            (casinoTransaction, payment) ->
    //                EnrichedCasinoTransaction.builder()
    //                    .casinoTransactionRequest(casinoTransaction)
    //                    .paymentRequest(payment)
    //                    .accountsProductRequest(createAccountsProductRequest(payment))
    //                    .build();
    //
    //    ValueJoiner<EnrichedCasinoTransaction, AccountsProductRequest, EnrichedCasinoTransaction>
    //        enrichmentJoiner =
    //            (enrichedCasinoTransaction, accountsProduct) -> {
    //              if (accountsProduct != null
    //                  && accountsProduct
    //                      .getDisplayId()
    //                      .equals(enrichedCasinoTransaction.getPaymentRequest().getSourceId())) {
    //                enrichedCasinoTransaction.setAccountsProductRequest(accountsProduct);
    //              }
    //              return enrichedCasinoTransaction;
    //            };
    //
    //    KStream<Long, EnrichedCasinoTransaction> enrichedCasinoTransactionStream =
    //        casinoTransactionRequestStream.join(
    //            paymentRequestStream.toStream(),
    //            casinoPaymentJoiner,
    //            JoinWindows.of(Duration.ofSeconds(3)),
    //            StreamJoined.with(Serdes.Long(), casinoTransactionRequestSerde,
    // paymentRequestSerde));
    //
    //    enrichedCasinoTransactionStream.leftJoin(
    //        accountsProductRequestTable,
    //        enrichmentJoiner,
    //        Joined.with(Serdes.Long(), enrichedCasinoTransactionSerde,
    // accountsProductRequestSerde));

    //    enrichedCasinoTransactionStream = enrichedCasinoTransactionStream
    //            .filter((key, value)->
    // (value.getCasinoTransactionRequest().getPaymentId().equals(value.getPaymentRequest().getId())) &&
    //
    // value.getAccountsProductRequest().getDisplayId().equals(value.getPaymentRequest().getSourceId()));

    //    enrichedCasinoTransactionStream.peek((key, value) -> System.out.println("OBJECT\n"+
    //            "key "+key+" value "+value
    //    ));



    //        enrichedCasinoTransactionStream.foreach((key, value) ->
    //                log.info("Key: "+key.toString() + " - "+" Value "+value.toString()));

    //    enrichedCasinoTransactionStream.peek(
    //        (key, value) ->
    //            System.out.println(
    //                "OBJECT\n"
    //                    + "Casino payment id: "
    //                    + value.getCasinoTransactionRequest().getPaymentId()
    //                    + " - paymentId: "
    //                    + value.getPaymentRequest().getId()
    //                    + "\n"
    //                    + "Account display id: "
    //                    + value.getAccountsProductRequest().getDisplayId()
    //                    + " - paymentSourceId: "
    //                    + value.getPaymentRequest().getSourceId()));

    return streamsBuilder.build();
  }

  private static AccountsProductRequest createAccountsProductRequest(
      PaymentRequest paymentRequest) {
    return AccountsProductRequest.builder().displayId(paymentRequest.getSourceId()).build();
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
}
