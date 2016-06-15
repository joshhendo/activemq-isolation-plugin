package test.org.apache.activemq.isolation;

import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.isolation.IsolationBroker;

import org.apache.activemq.isolation.exceptions.NoKeyException;
import org.apache.activemq.isolation.exceptions.NoLockException;
import org.apache.activemq.isolation.lock.LockProvider;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.nio.charset.StandardCharsets.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;

public class IsolationBrokerTest {
    private IsolationBroker isolationBroker;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.isolationBroker = new IsolationBroker(null, new LockProvider(), "./assets/definition.json");
    }

    @After
    public void tearDown() throws Exception {

    }

    private Message generateMockMessage(String content) {
        String messageIdString = java.util.UUID.randomUUID().toString();
        String correlationIdString = java.util.UUID.randomUUID().toString();
        String messageContent = content;
        byte[] messageContentBytes = messageContent.getBytes(US_ASCII);

        MessageId mockedMessageId = new MessageId(messageIdString);
        Message mockedMessage = mock(Message.class);

        when(mockedMessage.getMessageId()).thenReturn(mockedMessageId);
        when(mockedMessage.getCorrelationId()).thenReturn(correlationIdString);
        when(mockedMessage.getContent()).thenReturn(new ByteSequence(messageContentBytes));

        return mockedMessage;
    }

    private Message generateMockMessage() {
        return generateMockMessage("{\"message\": \"addUser\", \"userid\": \"12345678\"}");
    }

    private MessageAck generateMockMessageAck(MessageId messageId) {
        MessageAck mockMessageAck = mock(MessageAck.class);
        when(mockMessageAck.getLastMessageId()).thenReturn(messageId);
        return mockMessageAck;
    }

    @Test
    public void send() throws Exception {
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);
        Message mockedMessage = generateMockMessage();

        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockedMessage);
    }

    @Test
    public void EnsureNoExceptionThrownOnSameLockSameCorrelationId() throws Exception {
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);
        Message mockMessage = generateMockMessage();

        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage);
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage);
    }

    @Test
    public void EnsureExceptionThrownOnSameLockDifferentCorrelationId() throws Exception {
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);

        Message mockMessage1 = generateMockMessage();
        Message mockMessage2 = generateMockMessage();

        // This should be fine
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage1);

        // Different correlation ID, expect it to throw an exceptions
        exception.expect(NoLockException.class);
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage2);
    }

    @Test
    public void EnsureLockCanBeReAquiredAfterRelease() throws Exception {
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);
        ConsumerBrokerExchange mockedConsumerBrokerExchange = mock(ConsumerBrokerExchange.class);

        Message mockMessage1 = generateMockMessage();
        MessageId mockMessageId1 = mockMessage1.getMessageId();
        MessageAck mockMessageAck1 = mock(MessageAck.class);
        when(mockMessageAck1.getLastMessageId()).thenReturn(mockMessageId1);

        Message mockMessage2 = generateMockMessage();
        MessageId mockMessageId2 = mockMessage2.getMessageId();
        MessageAck mockMessageAck2 = mock(MessageAck.class);
        when(mockMessageAck2.getLastMessageId()).thenReturn(mockMessageId2);

        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage1);
        this.isolationBroker.processAcknowledge(mockedConsumerBrokerExchange, mockMessageAck1);

        // This should NOT throw an exceptions
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage2);
    }

    @Test
    // Test flow:
    // Step 1: Message 1 will get the lock
    // Step 2: Message 2 will fail but be placed on the queue
    // Step 3: Message 1 will release the lock, allowing it to be picked up by the next lock on the queue
    // Step 4: Message 3 will fail because, whilst the lock is free, the virtual queue gives preferences to Message 2
    // Step 5: Message 2 will get the lock as it is first on the queue
    // Step 6: Message 2 will release the lock
    // Step 7: Message 3 will get the lock as it is not held and there is nothing infront of it in the virtual queue
    public void EnsureVirtualQueueWorks() throws Exception {
        // Declarations
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);
        ConsumerBrokerExchange mockedConsumerBrokerExchange = mock(ConsumerBrokerExchange.class);

        Message mockMessage1 = generateMockMessage();
        MessageAck mockMessageAck1 = generateMockMessageAck(mockMessage1.getMessageId());
        Message mockMessage2 = generateMockMessage();
        MessageAck mockMessageAck2 = generateMockMessageAck(mockMessage2.getMessageId());
        Message mockMessage3 = generateMockMessage();
        MessageAck mockMessageAck3 = generateMockMessageAck(mockMessage3.getMessageId());

        // Step 1
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage1);

        // Step 2
        boolean caughtException1 = false;
        try {
            this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage2);
        } catch (NoLockException e) {
            caughtException1 = true;
        }
        assert(caughtException1 == true);

        // Step 3
        this.isolationBroker.processAcknowledge(mockedConsumerBrokerExchange, mockMessageAck1);

        // Step 4
        boolean caughtException2 = false;
        try {
            this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage3);
        } catch (NoLockException e) {
            caughtException2 = true;
        }
        assert(caughtException2 == true);

        // Step 5
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage2);

        // Step 6
        this.isolationBroker.processAcknowledge(mockedConsumerBrokerExchange, mockMessageAck2);

        // Step 7
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage3);
    }

    @Test
    public void EnsureThrowExceptionIfMessageDoesntContainKey() throws Exception {
        // Declarations
        ProducerBrokerExchange mockedProducerBrokerExchange = mock(ProducerBrokerExchange.class);

        // Missing the key 'userid'
        Message mockMessage1 = generateMockMessage("{\"message\": \"addUser\"}");

        exception.expect(NoKeyException.class);
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage1);
    }
}