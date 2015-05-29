package org.kaazing.messaging.driver.transport;

import org.kaazing.messaging.driver.message.DriverMessage;

import java.util.function.Consumer;

public interface TransportFactory
{
    public String getScheme();
    public TransportContext createTransportContext();

    //TODO(JAF): Implement a generic way to pass extra information like stream
    public SendingTransport createSendingTransport(TransportContext transportContext, String address);
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, String targetTransportHandleId);
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, Consumer<DriverMessage> messageHandler);

}
