package com.example.kafkastreams.ziqni;

import com.example.kafkastreams.request.CasinoTransactionRequest;
import lombok.*;

import java.time.Instant;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class CasinoZiqni {

  private Long memberRefId;
  private String action;
  private String entityRefId;
  private Integer sourceValue;
  private Instant transactionTimestamp;
  private String relatesToExternal;

  public CasinoZiqni process(CasinoTransactionRequest casinoTransactionRequest) {
    this.memberRefId = casinoTransactionRequest.getUuid();
    this.action = casinoTransactionRequest.getCurrency();
    this.entityRefId = casinoTransactionRequest.getStatus();
    this.sourceValue = casinoTransactionRequest.getAccountId();
    this.relatesToExternal = casinoTransactionRequest.getType();

    return this;
  }
}
