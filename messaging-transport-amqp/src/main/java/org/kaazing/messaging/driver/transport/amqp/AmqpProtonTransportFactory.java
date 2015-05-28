package org.kaazing.messaging.driver.transport.amqp;

import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.kaazing.messaging.driver.transport.TransportFactory;

import java.util.function.Consumer;

public class AmqpProtonTransportFactory implements TransportFactory
{

    @Override
    public String getScheme() {
        return "amqp";
    }

    @Override
    public TransportContext createTransportContext() {
        return new AmqpProtonTransportContext();
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream) {
        return new AmqpProtonSendingTransport((AmqpProtonTransportContext) transportContext, address);
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream, String targetTransportHandleId) {
        return new AmqpProtonSendingTransport((AmqpProtonTransportContext) transportContext, address, targetTransportHandleId);
    }

    @Override
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, int stream, Consumer<Message> messageHandler) {
        return new AmqpProtonReceivingTransport((AmqpProtonTransportContext) transportContext, address, messageHandler);
    }
}
