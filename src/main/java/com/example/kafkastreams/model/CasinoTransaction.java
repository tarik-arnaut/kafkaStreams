package com.example.kafkastreams.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CasinoTransaction {

  Long id;

  @JsonAlias("account_id")
  Integer accountId;

  String type;

  @JsonAlias("payment_id")
  Integer paymentId;

  Long uuid;
  String status;
  Integer amount;
  String currency;

  @JsonAlias("user_type")
  String userType;

  @JsonAlias("undo_control_id")
  Integer undoControlId;

  @JsonAlias("reverses_id")
  Integer reversesId;

  @JsonAlias("created_at")
  Long createdAt;

  @JsonAlias("updated_at")
  Long updatedAt;
}
