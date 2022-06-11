package com.example.kafkastreams.controller;

import com.example.kafkastreams.model.CasinoZiqni;
import com.example.kafkastreams.service.CasinoZiqniService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/casino-ziqni")
public class CasinoZiqniController {

  private final CasinoZiqniService casinoZiqniService;

  public CasinoZiqniController(CasinoZiqniService casinoZiqniService) {
    this.casinoZiqniService = casinoZiqniService;
  }

  @GetMapping(value = "/{casinoZiqniId}", produces = "application/json")
  public ResponseEntity<CasinoZiqni> getCasinoZiqniById(
      @PathVariable("casinoZiqniId") Long casinoZiqniId) {
    return ResponseEntity.ok(casinoZiqniService.getCasinoZiqniById(casinoZiqniId));
  }
}
