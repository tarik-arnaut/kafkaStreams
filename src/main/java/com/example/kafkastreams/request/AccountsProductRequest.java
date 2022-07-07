package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class AccountsProductRequest {

  Long id;

  @JsonAlias("display_id")
  String displayId;

  @JsonAlias("created_at")
  String createdAt;

  @JsonAlias("updated_at")
  String updatedAt;

  String provider;
  String category;
  String group;
  String aggregator;
  String name;
  String type;

  @JsonAlias("__deleted")
  boolean deleted;
}
