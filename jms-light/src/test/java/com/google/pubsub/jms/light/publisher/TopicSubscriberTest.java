package com.google.pubsub.jms.light.publisher;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;


import junit.framework.Assert;
import org.junit.Test;

/**
 * Test basic topic calls.
 *
 * @author Carter Page <carter@skyscreamer.org>
 */
public class TopicSubscriberTest extends AbstractJMSTest {
    @Test
    public void testTopics() throws JMSException, InterruptedException {
        Session session = createSession();
        Topic testTopic = session.createTopic("testTopic");
        MessageProducer producer = session.createProducer(testTopic);
        MessageConsumer consumer1 = session.createConsumer(testTopic);
        MessageConsumer consumer2 = session.createConsumer(testTopic);
        TextMessage testMessage = session.createTextMessage("024blah");
        producer.send(testMessage);
        TextMessage msgOut1 = (TextMessage)consumer1.receive(1000);
        TextMessage msgOut2 = (TextMessage)consumer2.receive(1000);
        Assert.assertEquals(testMessage.getText(), msgOut1.getText());
        Assert.assertEquals(testMessage.getText(), msgOut2.getText());
        producer.close();
        consumer1.close();
        consumer2.close();
        deleteTopic(testTopic);
    }

    @Test
    public void testNoLocal() throws JMSException {
        getConnection().close(); // Don't use the provided connection
        Connection conn1 = createConnection(getConnectionFactory());
        conn1.start();
        Connection conn2 = createConnection(getConnectionFactory());
        conn2.start();
        Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // TODO: Is topic bound to a session, or how can we handle this test?
        //Topic testTopic = new Topic("testTopic");
        Topic testTopic = session1.createTopic("testTopic");
        MessageProducer producer1 = session1.createProducer(testTopic);
        MessageProducer producer2 = session2.createProducer(testTopic);
        MessageConsumer consumer1 = session1.createConsumer(testTopic, null, true);
        MessageConsumer consumer2 = session2.createConsumer(testTopic, null, true);
        MessageConsumer consumer3 = session2.createConsumer(testTopic, null, false);

        TextMessage testMessage1 = session1.createTextMessage("msg123");
        producer1.send(testMessage1);
        TextMessage testMessage2 = session2.createTextMessage("anotherMsg456");
        producer2.send(testMessage2);

        Assert.assertEquals(testMessage2, (TextMessage)consumer1.receive(1000));
        Assert.assertNull((TextMessage)consumer1.receive(200));
        Assert.assertEquals(testMessage1, (TextMessage)consumer2.receive(1000));
        Assert.assertNull((TextMessage)consumer2.receive(200));
        TextMessage msgOut1 = (TextMessage)consumer3.receive(1000);
        TextMessage msgOut2 = (TextMessage)consumer3.receive(1000);
        compareTextMessages(new TextMessage[] {testMessage1, testMessage2}, new TextMessage[] {msgOut1, msgOut2});
        conn1.close();
        conn2.close();
    }
}
