package org.kaazing.messaging.common.command;

import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.transport.DiscoverableTransport;
import org.kaazing.messaging.common.transport.ReceivingTransport;
import org.kaazing.messaging.common.transport.SendingTransport;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

public class MessagingCommand {
    public static final int TYPE_ADD_SENDING_TRANSPORT = 1;
    public static final int TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT = 2;
    public static final int TYPE_ADD_RECEIVING_TRANSPORT = 3;
    public static final int TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT = 4;

    public static final int TYPE_CREATE_PRODUCER = 7;
    public static final int TYPE_DELETE_PRODUCER = 8;
    public static final int TYPE_CREATE_CONSUMER = 9;
    public static final int TYPE_DELETE_CONSUMER = 10;

    private int messageProducerIndex;
    private long messageProducerId;
    private MessageFlow messageFlow;
    private OneToOneConcurrentArrayQueue<Message> sendQueue;
    private OneToOneConcurrentArrayQueue<Message> freeQueue;

    private long messageConsumerId;
    private Consumer<Message> messageHandler;

    private int type;
    private SendingTransport sendingTransport;
    private Consumer<MessagingCommand> commandCompletedAction;


    public void setType(int type)
    {
        this.type = type;
    }

    public int getType()
    {
        return type;
    }

    public void setSendingTransport(SendingTransport sendingTransport)
    {
        this.sendingTransport = sendingTransport;
    }

    public SendingTransport getSendingTransport()
    {
        return sendingTransport;
    }

    public void setCommandCompletedAction(Consumer<MessagingCommand> commandCompletedAction)
    {
        this.commandCompletedAction = commandCompletedAction;
    }

    public Consumer<MessagingCommand> getCommandCompletedAction()
    {
        return commandCompletedAction;
    }

    public int getMessageProducerIndex() {
        return messageProducerIndex;
    }

    public void setMessageProducerIndex(int messageProducerIndex) {
        this.messageProducerIndex = messageProducerIndex;
    }

    public MessageFlow getMessageFlow() {
        return messageFlow;
    }

    public void setMessageFlow(MessageFlow messageFlow) {
        this.messageFlow = messageFlow;
    }

    public OneToOneConcurrentArrayQueue<Message> getSendQueue() {
        return sendQueue;
    }

    public void setSendQueue(OneToOneConcurrentArrayQueue<Message> sendQueue) {
        this.sendQueue = sendQueue;
    }

    public OneToOneConcurrentArrayQueue<Message> getFreeQueue() {
        return freeQueue;
    }

    public void setFreeQueue(OneToOneConcurrentArrayQueue<Message> freeQueue) {
        this.freeQueue = freeQueue;
    }

    public Consumer<Message> getMessageHandler() {
        return messageHandler;
    }

    public void setMessageHandler(Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;
    }

    public long getMessageProducerId() {
        return messageProducerId;
    }

    public void setMessageProducerId(long messageProducerId) {
        this.messageProducerId = messageProducerId;
    }

    public long getMessageConsumerId() {
        return messageConsumerId;
    }

    public void setMessageConsumerId(long messageConsumerId) {
        this.messageConsumerId = messageConsumerId;
    }
}
