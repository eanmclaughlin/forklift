package forklift.integration;

import com.github.dcshock.avro.schemas.StateCode;
import com.github.dcshock.avro.schemas.UserRegistered;
import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

public class AvroMessageTests extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageTests.class);

    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
    }

    @Test
    public void testComplexAvroMessageWithProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-avro-topic");
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("Eye", "producerProperty");
        producer.setProperties(producerProps);
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        // Start the consumer.
        c.listen();
        messageAsserts();
    }

    @Test
    public void testComplexAvroMessageWithoutProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-avro-topic");
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        // Start the consumer.
        c.listen();
        messageAsserts();
    }

    @Queue("forklift-avro-topic")
    public static class RegisteredAvroConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private UserRegistered value;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            System.out.println(Thread.currentThread().getName() + value.getState());
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }
}
