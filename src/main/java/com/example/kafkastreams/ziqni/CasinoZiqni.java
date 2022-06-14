package com.example.kafkastreams.ziqni;

import com.example.kafkastreams.request.CasinoTransactionRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CasinoZiqni {

  Long memberRefId;
  String action;
  String entityRefId;
  Integer sourceValue;
  Instant transactionTimestamp;
  String relatesToExternal;

  public CasinoZiqni process(CasinoTransactionRequest casinoTransactionRequest){
      this.memberRefId= casinoTransactionRequest.getUuid();
      this.action= casinoTransactionRequest.getCurrency();
      this.entityRefId= casinoTransactionRequest.getStatus();
      this.sourceValue= casinoTransactionRequest.getAccountId();
      this.relatesToExternal= casinoTransactionRequest.getType();

      return this;
  }
}
