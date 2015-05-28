package org.kaazing.messaging.common.command;

import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.message.Message;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

public class ClientCommand implements Command
{
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

    private Consumer<ClientCommand> commandCompletedAction;

    private final int type;

    public ClientCommand(int type)
    {
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    public int getMessageProducerIndex() {
        return messageProducerIndex;
    }

    public void setMessageProducerIndex(int messageProducerIndex) {
        this.messageProducerIndex = messageProducerIndex;
    }

    public long getMessageProducerId() {
        return messageProducerId;
    }

    public void setMessageProducerId(long messageProducerId) {
        this.messageProducerId = messageProducerId;
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


    public long getMessageConsumerId() {
        return messageConsumerId;
    }

    public void setMessageConsumerId(long messageConsumerId) {
        this.messageConsumerId = messageConsumerId;
    }

    public void setCommandCompletedAction(Consumer<ClientCommand> commandCompletedAction)
    {
        this.commandCompletedAction = commandCompletedAction;
    }

    public Consumer<ClientCommand> getCommandCompletedAction()
    {
        return commandCompletedAction;
    }
}
