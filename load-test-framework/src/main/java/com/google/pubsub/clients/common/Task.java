package com.google.pubsub.clients.common;

import com.google.pubsub.flic.common.LoadtestProto;

public interface Task extends Runnable {
  LoadtestProto.Distribution getDistribution();
}
