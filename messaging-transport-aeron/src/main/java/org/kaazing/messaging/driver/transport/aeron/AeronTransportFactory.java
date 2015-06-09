package org.kaazing.messaging.driver.transport.aeron;

import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.kaazing.messaging.driver.transport.TransportFactory;
import uk.co.real_logic.aeron.driver.uri.AeronUri;

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
    public SendingTransport createSendingTransport(TransportContext transportContext, String address) {
        int streamId = getStreamIdFromAddress(address);

        return new AeronSendingTransport((AeronTransportContext) transportContext, address, streamId);
    }

    @Override
    public SendingTransport createSendingTransport(TransportContext transportContext, String address,String targetTransportHandleId) {
        int streamId = getStreamIdFromAddress(address);
        return new AeronSendingTransport((AeronTransportContext) transportContext, address, streamId, targetTransportHandleId);
    }

    @Override
    public ReceivingTransport createReceivingTransport(TransportContext transportContext, String address, Consumer<DriverMessage> messageHandler)
    {
        int streamId = getStreamIdFromAddress(address);
        return new AeronReceivingTransport((AeronTransportContext) transportContext, address, streamId, messageHandler);
    }

    private int getStreamIdFromAddress(String address)
    {
        int streamId = 0;
        AeronUri aeronUri = AeronUri.parse(address);
        if(aeronUri.get("streamId") != null)
        {
            streamId = Integer.parseInt(aeronUri.get("streamId"));
        }
        return streamId;
    }
}
