package org.kaazing.messaging.driver.transport.netty.tcp;

import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.kaazing.messaging.driver.transport.TransportFactory;

import java.util.function.Consumer;

public class NettyTransportFactory implements TransportFactory
{

    @Override
    public String getScheme() {
        return "tcp";
    }

    @Override
    public TransportContext createTransportContext() {
        return new NettyTransportContext();
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address) {
        return new NettySendingTransport((NettyTransportContext) transportContext, address);
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address, String targetTransportHandleId) {
        return new NettySendingTransport((NettyTransportContext) transportContext, address, targetTransportHandleId);
    }

    @Override
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, Consumer<DriverMessage> messageHandler) {
        return new NettyReceivingTransport((NettyTransportContext) transportContext, address, messageHandler);
    }
}
