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
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.Publication;

public class AeronSendingTransport implements SendingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AeronSendingTransport.class);

    private final String channel;
    private final int streamId;
    private String targetTransportHandleId;
    private final Publication publication;
    private final AeronTransportContext aeronTransportContext;

    public AeronSendingTransport(AeronTransportContext aeronTransportContext, String channel, int streamId)
    {
        this.aeronTransportContext = aeronTransportContext;
        this.channel = channel;
        this.streamId = streamId;

        LOGGER.info("Creating Aeron publication on channel={}, stream={}", channel, streamId);

        this.publication = aeronTransportContext.getAeron().addPublication(channel, streamId);
    }

    public AeronSendingTransport(AeronTransportContext aeronTransportContext, String channel, int streamId, String targetTransportHandleId)
    {
        this.aeronTransportContext = aeronTransportContext;
        this.channel = channel;
        this.streamId = streamId;

        LOGGER.info("Creating Aeron publication on channel={}, stream={}", channel, streamId);

        this.publication = aeronTransportContext.getAeron().addPublication(channel, streamId);
        this.targetTransportHandleId = targetTransportHandleId;
    }

    @Override
    public TransportContext getTransportContext()
    {
        return aeronTransportContext;
    }

    @Override
    public void submit(DriverMessage driverMessage)
    {
        throw new UnsupportedOperationException("blocking submit method is not supported with this transport");
    }

    @Override
    public long offer(DriverMessage driverMessage)
    {
        LOGGER.debug("Sending message of length={} with Aeron publication on channel={}, stream={}", driverMessage.getBufferLength(), channel, streamId);
        return publication.offer(driverMessage.getBuffer(), driverMessage.getBufferOffset(), driverMessage.getBufferLength());
    }

    @Override
    public void close()
    {
        publication.close();
    }

    @Override
    public String getTargetTransportHandleId()
    {
        return targetTransportHandleId;
    }

}
