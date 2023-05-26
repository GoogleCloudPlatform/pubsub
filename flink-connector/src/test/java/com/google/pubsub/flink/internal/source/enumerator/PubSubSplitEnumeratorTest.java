/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.flink.internal.source.enumerator;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitEnumeratorTest {
  private static final ProjectSubscriptionName SUBSCRIPTION_NAME =
      ProjectSubscriptionName.of("project", "subscription");

  @Mock SplitEnumeratorContext<SubscriptionSplit> mockContext;

  PubSubSplitEnumerator splitEnumerator;

  private class SplitsAssignmentMatcher implements ArgumentMatcher<SplitsAssignment> {
    Set<Integer> expectedReaders;

    public SplitsAssignmentMatcher(List<Integer> expectedReaders) {
      this.expectedReaders = new HashSet<>(expectedReaders);
    }

    @Override
    public boolean matches(SplitsAssignment assignment) {
      Map<Integer, List<SubscriptionSplit>> assignments = assignment.assignment();
      if (assignments.size() != expectedReaders.size()) {
        return false;
      }
      for (Map.Entry<Integer, List<SubscriptionSplit>> entry : assignments.entrySet()) {
        List<SubscriptionSplit> splits = entry.getValue();
        if (!expectedReaders.contains(entry.getKey())
            || splits.size() != 1
            || !splits.get(0).subscriptionName().toString().equals(SUBSCRIPTION_NAME.toString())) {
          return false;
        }
      }
      return true;
    }
  }

  @Before
  public void doBeforeEachTest() {
    splitEnumerator =
        new PubSubSplitEnumerator(
            SUBSCRIPTION_NAME, mockContext, new HashMap<Integer, SubscriptionSplit>());
  }

  private Map<Integer, ReaderInfo> createRegisteredReaders(List<Integer> readers) {
    Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    for (Integer reader : readers) {
      registeredReaders.put(reader, new ReaderInfo(reader, "location"));
    }
    return registeredReaders;
  }

  private List<Integer> readersFromCheckpoint(PubSubEnumeratorCheckpoint checkpoint) {
    List<Integer> readers = new ArrayList<>();
    checkpoint.getAssignmentsList().forEach(assignment -> readers.add(assignment.getSubtask()));
    return readers;
  }

  @Test
  public void addReader_generatesNewAssignment() throws Throwable {
    when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));

    splitEnumerator.addReader(0);
    verify(mockContext).registeredReaders();
    verify(mockContext).assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(0))));

    assertThat(readersFromCheckpoint(splitEnumerator.snapshotState(0L)))
        .isEqualTo(Arrays.asList(0));
  }

  @Test
  public void multipleReaders_multipleAssignments() throws Throwable {
    when(mockContext.registeredReaders())
        .thenReturn(createRegisteredReaders(Arrays.asList(0)))
        .thenReturn(createRegisteredReaders(Arrays.asList(0, 1)));

    splitEnumerator.addReader(0);
    splitEnumerator.addReader(1);
    verify(mockContext, times(2)).registeredReaders();
    verify(mockContext).assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(0))));
    verify(mockContext).assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(1))));

    assertThat(readersFromCheckpoint(splitEnumerator.snapshotState(0L)))
        .isEqualTo(Arrays.asList(0, 1));
  }

  @Test
  public void sameReader_noNewAssignment() throws Throwable {
    when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));

    splitEnumerator.addReader(0);
    splitEnumerator.addReader(0);
    verify(mockContext, times(2)).registeredReaders();
    verify(mockContext).assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(0))));

    assertThat(readersFromCheckpoint(splitEnumerator.snapshotState(0L)))
        .isEqualTo(Arrays.asList(0));
  }

  @Test
  public void addSplitsBack_removesReader() throws Throwable {
    when(mockContext.registeredReaders())
        .thenReturn(createRegisteredReaders(Arrays.asList(0)))
        .thenReturn(createRegisteredReaders(new ArrayList<>()));

    splitEnumerator.addReader(0);
    splitEnumerator.addSplitsBack(new ArrayList<>(), 0);
    verify(mockContext, times(2)).registeredReaders();
    verify(mockContext).assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(0))));

    assertThat(readersFromCheckpoint(splitEnumerator.snapshotState(0L))).isEmpty();
  }

  @Test
  public void addSplitsBack_readerRecovers() throws Throwable {
    when(mockContext.registeredReaders()).thenReturn(createRegisteredReaders(Arrays.asList(0)));

    splitEnumerator.addReader(0);
    splitEnumerator.addSplitsBack(new ArrayList<>(), 0);
    verify(mockContext, times(2)).registeredReaders();
    verify(mockContext, times(2))
        .assignSplits(argThat(new SplitsAssignmentMatcher(Arrays.asList(0))));

    assertThat(readersFromCheckpoint(splitEnumerator.snapshotState(0L)))
        .isEqualTo(Arrays.asList(0));
  }
}
