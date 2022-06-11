package com.example.kafkastreams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;

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

  public CasinoZiqni process(CasinoTransaction casinoTransaction){
      this.memberRefId=casinoTransaction.uuid;
      this.action=casinoTransaction.currency;
      this.entityRefId=casinoTransaction.status;
      this.sourceValue=casinoTransaction.accountId;
      this.relatesToExternal=casinoTransaction.type;

      return this;
  }
}
