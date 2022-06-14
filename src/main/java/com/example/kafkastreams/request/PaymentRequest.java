package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentRequest {

  Long id;
  String uuid;

  @JsonAlias("usr_origin")
  String usrOrigin;

  @JsonAlias("usr_uuid")
  String usrUuid;

  @JsonAlias("source_id")
  String sourceId;

  @JsonAlias("ref_id")
  String refId;

  int status;

  @JsonAlias("created_at")
  String createdAt;

  int strategy;

  @JsonAlias("creator_id")
  int creatorId;
}
