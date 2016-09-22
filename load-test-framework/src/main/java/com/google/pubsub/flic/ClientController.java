// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.flic;

public class ClientController {
  public class Client {
    public enum ClientType {
      CPS_VENEER,
      CPS_GRPC,
      KAFKA
    }
    private ClientType clientType;
    private string networkAddress;
    public Client(ClientType clientType, string networkAddress) {
      this.clientType = clientType;
      this.networkAddress = networkAddress;
    }
  }
  private List<Client> clients;
  public ClientController(List<Client> clients) {
    this.clients = clients;
    // start the jobs
    // then send RPCs
    // then wait, print stats
    // close jobs
  }

  // We re-upload all of our files to ensure everything is up to date, and since it is
  // otherwise difficult to ensure the files in Storage are up to date with our versions
  void initializeGCEProject(string projectName) {
    // here we can set up Storage / Metadata / InstanceTemplate
    Bucket loadtestBucket = storage.buckets().get("cloud-pubsub-loadtest").execute();
    if (loadtestBucket == null) {
      logger.onInfo().log("Bucket missing, creating a new bucket.");
      loadtestBucket = storage.buckets().insert(projectName, new Bucket()
        .setName("cloud-pubsub-loadtest")).execute();
    }
    if (loadtestBucket == null) {
      logger.onError().log("Unable to create a storage bucket!");
      throw Exception("Unable to create a storage bucket!");
    }

    for (GCEFile file : files) {
      StorageObject fileObject = storage.objects.get("cloud-pubsub-loadtest", file.getName()).execute();
      if (fileObject != null) {
        // Delete if it exists
        storageService.Objects.Delete("cloud-pubsub-loadtest", file.getName()).Execute();
      }
      fileObject = storage.objects().insert("cloud-pubsub-loadtests", null,
          new InputStreamContent("application/octet-stream", file.getInputStream()))
          .setName(file.getName()).execute();
    }
  }

  void createClientsGCE() {
    // this creates the instanceTemplate that we will clone GCE from if need be
    // and then creates the instance.
    // name = 'cloud-pubsub-load-test-gce-cps'
    // pr
    // properties.disks[0].initializeParams.sourceImage = projects/debian-cloud/global/images/debian-7-wheezy-vYYYYMMDD
    InstanceTemplate content = new InstanceTemplate();
    Compute.InstanceTemplates.Insert request =
        computeService.instanceTemplates().insert(project, content);
    Operation response = request.execute();

  }

  void startClients() {
    for (Client client : clients) {
      client.start();
    }
  }
}
