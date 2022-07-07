package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
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
