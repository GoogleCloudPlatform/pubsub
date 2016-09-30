package com.google.pubsub.flic.controllers;

public class ClientParams {
  Client.ClientType clientType;
  String subscription;

  public ClientParams(Client.ClientType clientType, String subscription) {
    this.clientType = clientType;
    this.subscription = subscription;
  }
}