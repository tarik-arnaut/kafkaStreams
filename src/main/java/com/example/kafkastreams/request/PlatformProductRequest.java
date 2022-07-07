package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class PlatformProductRequest {
  private Long id;

  @JsonAlias("group_id")
  private int groupId;

  private String name;
  private String uuid;
  private String description;

  @JsonAlias("product_provider_id")
  private String productProviderId;
}
