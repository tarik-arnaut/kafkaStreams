package com.example.kafkastreams.enriched;

import com.example.kafkastreams.request.AccountsProductRequest;
import com.example.kafkastreams.request.CasinoTransactionRequest;
import com.example.kafkastreams.request.PaymentRequest;
import com.example.kafkastreams.request.UserRequest;
import lombok.*;

@Getter
@Builder
@Setter
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedCasinoTransaction {
  private CasinoTransactionRequest casinoTransactionRequest;
  private PaymentRequest paymentRequest;
  private AccountsProductRequest accountsProductRequest;
  private UserRequest userRequest;

}
