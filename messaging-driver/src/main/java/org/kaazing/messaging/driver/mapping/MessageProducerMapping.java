package org.kaazing.messaging.driver.mapping;

import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.discovery.DiscoveryEvent;
import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.common.collections.AtomicArrayWithArg;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class MessageProducerMapping
{
    private static final int PRODUCER_COMMAND_QUEUE_DEFAULT_CAPACITY = 128;
    private final long id = ThreadLocalRandom.current().nextLong();
    private final MessageFlow messageFlow;
    private final int index;
    private final AtomicArrayWithArg<SendingTransport, DriverMessage> sendingTransports = new AtomicArrayWithArg<>();
    private final OneToOneConcurrentArrayQueue<DriverMessage> sendQueue = new OneToOneConcurrentArrayQueue<>(PRODUCER_COMMAND_QUEUE_DEFAULT_CAPACITY);

    //TODO(JAF): May need to switch to another data structure if multiple threads are being used to access the free list or offer to it
    private final OneToOneConcurrentArrayQueue<DriverMessage> freeQueue = new OneToOneConcurrentArrayQueue<>(PRODUCER_COMMAND_QUEUE_DEFAULT_CAPACITY);

    private Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction;

    public MessageProducerMapping(MessageFlow messageFlow, int index)
    {
        this.messageFlow = messageFlow;
        this.index = index;
        for(int i = 0; i < PRODUCER_COMMAND_QUEUE_DEFAULT_CAPACITY; i++)
        {
            freeQueue.offer(new DriverMessage(DriverMessage.DEFAULT_BUFFER_SIZE));
        }
    }

    public long getId()
    {
        return id;
    }

    public MessageFlow getMessageFlow()
    {
        return messageFlow;
    }

    public AtomicArrayWithArg<SendingTransport, DriverMessage> getSendingTransports()
    {
        return sendingTransports;
    }

    public OneToOneConcurrentArrayQueue<DriverMessage> getSendQueue()
    {
        return sendQueue;
    }

    public OneToOneConcurrentArrayQueue<DriverMessage> getFreeQueue()
    {
        return freeQueue;
    }

    public int getIndex()
    {
        return index;
    }

    public void setDiscoveredTransportsAction(Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction)
    {
        this.discoveredTransportsAction = discoveredTransportsAction;
    }

    public Consumer<DiscoveryEvent<DiscoverableTransport>> getDiscoveredTransportsAction()
    {
        return discoveredTransportsAction;
    }
}
