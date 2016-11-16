### Introduction

Code samples for google3/third_party/java_src/cloud/pubsub/project/client.

(NOTE) This client includes only data plane operations, publishing and
pulling messages, and it is based on our gRPC beta release.

(ALPHA) This library uses features that are part of an invitation-only
release of the underlying Cloud Pub/Sub API. The library will generate
errors unless you have access to this API. This restriction should be
relaxed in the near future. Please contact cloud-pubsub@google.com with any
questions in the meantime.

### How to use

blaze run third_party/java_src/cloud/pubsub/project:client_[publisher|subscriber]_samples  -- [your topic|subscription]
