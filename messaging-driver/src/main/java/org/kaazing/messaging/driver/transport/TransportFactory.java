package org.kaazing.messaging.driver.transport;

import org.kaazing.messaging.common.message.Message;

import java.util.function.Consumer;

public interface TransportFactory
{
    public String getScheme();
    public TransportContext createTransportContext();

    //TODO(JAF): Implement a generic way to pass extra information like stream
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream);
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, int stream, String targetTransportHandleId);
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, int stream, Consumer<Message> messageHandler);

}
