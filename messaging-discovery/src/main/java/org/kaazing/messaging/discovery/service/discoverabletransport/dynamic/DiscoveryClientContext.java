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
package org.kaazing.messaging.discovery.service.discoverabletransport.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.*;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class DiscoveryClientContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryClientContext.class);

    private static final int STREAM_ID = DiscoveryConfiguration.STREAM_ID;
    //TODO(JAF): Implement a way to configure the incoming and outgoing channel
    private static final String INCOMING_CHANNEL = "aeron:udp?remote=127.0.0.1:40000";
    //private static final String OUTGOING_CHANNEL_DEFAULT = "aeron:udp?remote=127.0.0.1:40001";
    private static final int FRAGMENT_COUNT_LIMIT = DiscoveryConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final IdleStrategy idleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
    private static final int HEARTBEAT_INTERVAL_SECONDS = 1;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private static final ClientSerializer serializer = new ClientSerializer();
    private static final ClientDeserializer deserializer = new ClientDeserializer();
    private final Client client = new Client(INCOMING_CHANNEL, STREAM_ID);
    private Publication publication = null;
    private Subscription subscription = null;
    private Aeron aeron = null;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private long lastRevisionSent = -1;
    private long lastSentTimestamp = -1;
    private Consumer<ClientDiscoveryEvent> clientDiscoveryListener = null;
    private ThreadLocal<ClientDiscoveryEvent> tlClientDiscoveryEvent = new ThreadLocal<>().withInitial(() -> new ClientDiscoveryEvent());
    private final String outgoingChannel;

    private Long2ObjectHashMap<Client> remoteClientMap = new Long2ObjectHashMap<>();

    public DiscoveryClientContext(String outgoingChannel)
    {
        this.outgoingChannel = outgoingChannel;
    }

    public void setClientDiscoveryListener(Consumer<ClientDiscoveryEvent> listener)
    {
        this.clientDiscoveryListener = listener;
    }
    public void unsetClientDiscoveryListener()
    {
        this.clientDiscoveryListener = null;
    }
    public Client getClient()
    {
        return client;
    }

    public void start()
    {


        final Aeron.Context ctx = new Aeron.Context();
        //if (EMBEDDED_MEDIA_DRIVER)
        //{
        //    ctx.dirName(driver.contextDirName());
        //}

        aeron = Aeron.connect(ctx);
        publication = aeron.addPublication(outgoingChannel, STREAM_ID);
        LOGGER.info("Publishing to channel={}, stream={} to produce discovery events", outgoingChannel, STREAM_ID);

        subscription = aeron.addSubscription(INCOMING_CHANNEL, STREAM_ID, dataHandler);
        LOGGER.info("Subscribing to channel={}, stream={} to consume discovery events", INCOMING_CHANNEL, STREAM_ID);

        executor.execute(() -> runDiscoveryLoop());
    }

    protected long sendNewRevision()
    {
        int bufferLength = serializer.serialize(BUFFER, client, Client.FULL_TYPE);
        return publication.offer(BUFFER, 0, bufferLength);
    }

    protected long sendHeartbeat()
    {
        int bufferLength = serializer.serialize(BUFFER, client, Client.REVISION_ONLY_TYPE);
        return publication.offer(BUFFER, 0, bufferLength);
    }

    protected void runDiscoveryLoop()
    {
        while (running.get())
        {
            long clientRevision = client.getRevision();
            if (lastRevisionSent < clientRevision)
            {
                if (sendNewRevision() >= 0L)
                {
                    lastRevisionSent = clientRevision;
                    lastSentTimestamp = System.currentTimeMillis();
                }
            } else if (System.currentTimeMillis() - lastSentTimestamp > TimeUnit.SECONDS.toMillis(HEARTBEAT_INTERVAL_SECONDS))
            {
                if (sendHeartbeat() >= 0L)
                {
                    lastRevisionSent = clientRevision;
                    lastSentTimestamp = System.currentTimeMillis();
                }
            }
            final int fragmentsRead = subscription.poll(FRAGMENT_COUNT_LIMIT);
            idleStrategy.idle(fragmentsRead);
        }
    }

    public void stop()
    {
        running.set(false);
        executor.shutdown();
        try
        {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        CloseHelper.quietClose(subscription);
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(aeron);
    }

    private final DataHandler dataHandler = new DataHandler()
    {
        @Override
        public void onData(DirectBuffer buffer, int offset, int length, Header header)
        {
            Client remoteClient = deserializer.deserialize(buffer, offset);
            LOGGER.debug("Received discovery event from client instance={}, revision={}", remoteClient.getInstanceId(), remoteClient.getRevision());

            if(remoteClient.getContentType() == Client.FULL_TYPE)
            {
                Client existingRemoteClient = remoteClientMap.putIfAbsent(remoteClient.getInstanceId(), remoteClient);
                if(existingRemoteClient != null && existingRemoteClient.getRevision() >= remoteClient.getRevision())
                {
                    //TODO(JAF): Log that we have already got this revision
                }
                else
                {
                    remoteClientMap.put(remoteClient.getInstanceId(), remoteClient);
                    Consumer<ClientDiscoveryEvent> discoveryEventListener = clientDiscoveryListener;
                    if(discoveryEventListener != null)
                    {
                        ClientDiscoveryEvent clientDiscoveryEvent = tlClientDiscoveryEvent.get();
                        clientDiscoveryEvent.setExistingClientInfo(existingRemoteClient);
                        clientDiscoveryEvent.setNewClientInfo(remoteClient);
                        discoveryEventListener.accept(clientDiscoveryEvent);
                    }
                }

            }
            else
            {
                //TODO(JAF): Log heartbeats if needed
            }
        }
    };
}
