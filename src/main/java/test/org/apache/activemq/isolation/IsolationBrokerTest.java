package test.org.apache.activemq.isolation;

import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.isolation.IsolationBroker;

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
        this.isolationBroker = new IsolationBroker(null, new LockProvider(), new ArrayList<String>());
    }

    @After
    public void tearDown() throws Exception {

    }

    private Message generateMockMessage() {
        String messageIdString = java.util.UUID.randomUUID().toString();
        String correlationIdString = java.util.UUID.randomUUID().toString();
        String messageContent = "{\"message\": \"addUser\", \"userid\": \"12345678\"}";
        byte[] messageContentBytes = messageContent.getBytes(US_ASCII);

        MessageId mockedMessageId = new MessageId(messageIdString);
        Message mockedMessage = mock(Message.class);

        when(mockedMessage.getMessageId()).thenReturn(mockedMessageId);
        when(mockedMessage.getCorrelationId()).thenReturn(correlationIdString);
        when(mockedMessage.getContent()).thenReturn(new ByteSequence(messageContentBytes));

        return mockedMessage;
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
        when(mockMessageAck1.getFirstMessageId()).thenReturn(mockMessageId1);

        Message mockMessage2 = generateMockMessage();
        MessageId mockMessageId2 = mockMessage2.getMessageId();
        MessageAck mockMessageAck2 = mock(MessageAck.class);
        when(mockMessageAck2.getFirstMessageId()).thenReturn(mockMessageId2);

        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage1);
        this.isolationBroker.processAcknowledge(mockedConsumerBrokerExchange, mockMessageAck1);

        // This should NOT throw an exceptions
        this.isolationBroker.processMessage(mockedProducerBrokerExchange, mockMessage2);
    }

}