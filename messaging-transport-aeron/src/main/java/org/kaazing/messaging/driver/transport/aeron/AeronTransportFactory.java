package org.kaazing.messaging.driver.transport.aeron;

import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.kaazing.messaging.driver.transport.TransportFactory;

import java.util.function.Consumer;

public class AeronTransportFactory implements TransportFactory
{
    @Override
    public String getScheme() {
        return "aeron";
    }

    @Override
    public TransportContext createTransportContext() {
        return new AeronTransportContext();
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream) {
        return new AeronSendingTransport((AeronTransportContext) transportContext, address, stream);
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream, String targetTransportHandleId) {
        return new AeronSendingTransport((AeronTransportContext) transportContext, address, stream, targetTransportHandleId);
    }

    @Override
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, int stream, Consumer<Message> messageHandler) {
        return new AeronReceivingTransport((AeronTransportContext) transportContext, address, stream, messageHandler);
    }
}
