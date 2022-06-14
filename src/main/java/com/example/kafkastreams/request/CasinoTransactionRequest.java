package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
public class CasinoTransactionRequest {

  Long id;

  @JsonAlias("account_id")
  Integer accountId;

  String type;

  @JsonAlias("payment_id")
  Long paymentId;

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
