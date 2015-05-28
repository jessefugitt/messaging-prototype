package org.kaazing.messaging.driver.mapping;

import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.concurrent.ThreadLocalRandom;

public class MessageConsumerMapping
{
    private final long id = ThreadLocalRandom.current().nextLong();
    private final MessageFlow messageFlow;
    private final AtomicArray<ReceivingTransport> receivingTransports = new AtomicArray<>();

    public MessageConsumerMapping(MessageFlow messageFlow)
    {
        this.messageFlow = messageFlow;
    }

    public long getId()
    {
        return id;
    }

    public AtomicArray<ReceivingTransport> getReceivingTransports()
    {
        return receivingTransports;
    }
}
