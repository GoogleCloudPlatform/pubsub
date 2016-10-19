package com.google.pubsub.flic.controllers;

public class ClientParams {
  private final Client.ClientType clientType;
  final String subscription;

  public ClientParams(Client.ClientType clientType, String subscription) {
    this.clientType = clientType;
    this.subscription = subscription;
  }

  /**
   * @return the clientType
   */
  public Client.ClientType getClientType() {
    return clientType;
  }
}