package com.example.kafkastreams.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class UserRequest {

  Long id;
  String nickname;
  String email;
  String uuid;

  @JsonAlias("club_uuid")
  String clubUuid;

  String gender;
  String language;
  String country;
  String type;

  @JsonAlias("created_at")
  String createdAt;

  @JsonAlias("updated_at")
  String updatedAt;
}
