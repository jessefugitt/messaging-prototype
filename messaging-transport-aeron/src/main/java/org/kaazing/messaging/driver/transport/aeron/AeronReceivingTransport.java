/*
    Copyright 2015 Kaazing Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package org.kaazing.messaging.driver.transport.aeron;

import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.common.transport.TransportHandle;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.UUID;
import java.util.function.Consumer;

public class AeronReceivingTransport implements ReceivingTransport, DataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AeronReceivingTransport.class);

    private final String channel;
    private final int streamId;
    private final Subscription subscription;
    private final AeronTransportContext aeronTransportContext;
    private final Consumer<DriverMessage> messageHandler;
    private final TransportHandle handle;
    private DiscoverableTransport discoverableTransport;
    private final ThreadLocal<DriverMessage> tlMessage = new ThreadLocal<>().withInitial(() -> new DriverMessage());

    public AeronReceivingTransport(AeronTransportContext aeronTransportContext, String channel, int streamId, Consumer<DriverMessage> messageHandler)
    {
        this.aeronTransportContext = aeronTransportContext;
        this.channel = channel;
        this.streamId = streamId;

        LOGGER.info("Creating Aeron subscription on channel={}, stream={}", channel, streamId);

        this.subscription = aeronTransportContext.getAeron().addSubscription(channel, streamId, this);
        this.messageHandler = messageHandler;
        this.handle = new TransportHandle(channel, "aeron", UUID.randomUUID().toString());
    }

    @Override
    public TransportContext getTransportContext()
    {
        return aeronTransportContext;
    }

    @Override
    public void setDiscoverableTransport(DiscoverableTransport discoverableTransport)
    {
        this.discoverableTransport = discoverableTransport;
    }

    @Override
    public DiscoverableTransport getDiscoverableTransport()
    {
        return discoverableTransport;
    }

    @Override
    public TransportHandle getHandle()
    {
        return handle;
    }

    @Override
    public void close()
    {
        subscription.close();
    }

    @Override
    public void onData(DirectBuffer buffer, int offset, int length, Header header)
    {
        LOGGER.debug("Received message of length={} with subscription on channel={}, stream={}", length, channel, streamId);
        DriverMessage driverMessage = tlMessage.get();
        driverMessage.setBuffer(buffer);
        driverMessage.setBufferOffset(offset);
        driverMessage.setBufferLength(length);
        //TODO(JAF): Map header information into message metadata

        messageHandler.accept(driverMessage);
    }

    @Override
    public int poll(final int limit)
    {
        return subscription.poll(limit);
    }

    @Override
    public boolean isPollable()
    {
        return true;
    }
}
