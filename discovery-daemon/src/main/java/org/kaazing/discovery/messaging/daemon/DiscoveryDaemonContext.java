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
package org.kaazing.discovery.messaging.daemon;

import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.discovery.Discoverable;
import org.kaazing.messaging.discovery.service.discoverabletransport.dynamic.Client;
import org.kaazing.messaging.discovery.service.discoverabletransport.dynamic.ClientDeserializer;
import org.kaazing.messaging.discovery.service.discoverabletransport.dynamic.ClientSerializer;
import org.kaazing.messaging.discovery.service.discoverabletransport.dynamic.DiscoveryConfiguration;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiscoveryDaemonContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryDaemonContext.class);

    private static final int STREAM_ID = DiscoveryConfiguration.STREAM_ID;
    private static final String INCOMING_CHANNEL = "aeron:udp?remote=127.0.0.1:40001";
    private static final boolean EMBEDDED_MEDIA_DRIVER = DiscoveryConfiguration.EMBEDDED_MEDIA_DRIVER;
    private static final int FRAGMENT_COUNT_LIMIT = DiscoveryConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final IdleStrategy idleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private static final ClientSerializer serializer = new ClientSerializer();
    private static final ClientDeserializer deserializer = new ClientDeserializer();
    private MediaDriver driver = null;
    private Subscription subscription = null;
    private Aeron aeron = null;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private long lastRevisionSent = -1;
    private long lastSentTimestamp = -1;

    private final Long2ObjectHashMap<Client> instanceIdToClientMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Publication> instanceIdToPublicationMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<List<Client>> listenKeyToClientsMap = new Long2ObjectHashMap<List<Client>>();
    private final Long2ObjectHashMap<List<Client>> valueToClientsMap = new Long2ObjectHashMap<List<Client>>();

    public void removeClientValue(Client client, long key, List<Long> updatedKeys)
    {
        List<Client> valuesList = valueToClientsMap.computeIfAbsent(key, (k) -> new ArrayList<Client>());
        boolean removed = valuesList.remove(client);
        if(removed && !updatedKeys.contains(key))
        {
            updatedKeys.add(key);
        }
    }

    public void addClientValue(Client client, long key, List<Long> updatedKeys)
    {
        List<Client> valuesList = valueToClientsMap.computeIfAbsent(key, (k) -> new ArrayList<Client>());
        valuesList.add(client);
        if(!updatedKeys.contains(key))
        {
            updatedKeys.add(key);
        }
    }


    public void removeClientListenKey(Client client, long key)
    {
        List<Client> interestList = valueToClientsMap.computeIfAbsent(key, (k) -> new ArrayList<Client>());
        interestList.remove(client);
    }

    public void addClientListenKey(Client client, long key)
    {
        List<Client> interestList = listenKeyToClientsMap.computeIfAbsent(key, (k) -> new ArrayList<Client>());
        interestList.add(client);
    }


    public void notifyListeners(List<Long> keys, Client newClient)
    {
        List<Client> alreadyNotifiedClients = new ArrayList<>();
        for(int i = 0; i < keys.size(); i++)
        {
            long key = keys.get(i);
            List<Client> interestList = listenKeyToClientsMap.get(key);
            if(interestList != null)
            {
                for(int j = 0; j < interestList.size(); j++)
                {
                    Client clientToNotify = interestList.get(j);
                    if(!alreadyNotifiedClients.contains(clientToNotify))
                    {
                        Publication publication = instanceIdToPublicationMap.get(clientToNotify.getInstanceId());
                        forwardNewRevision(publication, newClient);
                        alreadyNotifiedClients.add(clientToNotify);
                    }
                }
            }
        }
    }

    public void notifyNewClient(Client newClient)
    {
        AtomicArray<String> newClientListenKeys = newClient.getListenKeys();
        List<Client> alreadyMatchedClients = new ArrayList<>();

        if(newClientListenKeys != null) {
            final Publication newClientPublication = instanceIdToPublicationMap.get(newClient.getInstanceId());
            //TODO(JAF): Refactor to avoid capturing
            newClientListenKeys.forEach((key) ->
            {
                List<Client> existingClients = valueToClientsMap.get(key.hashCode());
                if(existingClients != null) {
                    existingClients.forEach((client) ->
                    {
                        if (!alreadyMatchedClients.contains(client)) {
                            forwardNewRevision(newClientPublication, client);
                            alreadyMatchedClients.add(client);
                        }
                    });
                }
            });
        }

    }


    public void start()
    {
        if (EMBEDDED_MEDIA_DRIVER)
        {
            MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
            mediaDriverContext.dirsDeleteOnExit(true);
            driver = MediaDriver.launchEmbedded(mediaDriverContext);
        }

        final Aeron.Context ctx = new Aeron.Context();
        if (EMBEDDED_MEDIA_DRIVER)
        {
            ctx.dirName(driver.contextDirName());
        }

        aeron = Aeron.connect(ctx);

        subscription = aeron.addSubscription(INCOMING_CHANNEL, STREAM_ID);
        LOGGER.info("Subscribing to channel={}, stream={} to consume discovery events", INCOMING_CHANNEL, STREAM_ID);

        executor.execute(() -> runDiscoveryLoop());
    }

    protected long forwardNewRevision(Publication publication, Client newClient)
    {
        int bufferLength = serializer.serialize(BUFFER, newClient, Client.FULL_TYPE);
        return publication.offer(BUFFER, 0, bufferLength);
    }

    protected long sendHeartbeat(Client existingClient)
    {
        Publication publication = instanceIdToPublicationMap.get(existingClient.getInstanceId());
        int bufferLength = serializer.serialize(BUFFER, existingClient, Client.REVISION_ONLY_TYPE);
        return publication.offer(BUFFER, 0, bufferLength);
    }

    protected void runDiscoveryLoop()
    {
        while (running.get())
        {
            //TODO(JAF): Send heartbeats to clients periodically
            /* foreach existingClient
            if (System.currentTimeMillis() - lastSentTimestamp > TimeUnit.SECONDS.toMillis(1))
            {
                if (sendHeartbeat() >= 0L)
                {
                    lastSentTimestamp = System.currentTimeMillis();
                }
            }
            sendHeartbeat(existingClient);
            */

            final int fragmentsRead = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
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
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(driver);
    }

    private final FragmentHandler fragmentHandler = new FragmentHandler()
    {
        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
        {
            Client newClient = deserializer.deserialize(buffer, offset);
            LOGGER.debug("Received discovery event from client instance={}, revision={}", newClient.getInstanceId(), newClient.getRevision());

            if(newClient.getContentType() == Client.FULL_TYPE)
            {
                Client existingClient = instanceIdToClientMap.putIfAbsent(newClient.getInstanceId(), newClient);
                if(existingClient != null && existingClient.getRevision() >= newClient.getRevision())
                {
                    LOGGER.debug("Current revision={} for client instance={} is more current than incoming revision={}",
                            existingClient.getRevision(), existingClient.getInstanceId(), newClient.getRevision());
                }
                else
                {
                    instanceIdToClientMap.put(newClient.getInstanceId(), newClient);
                    Publication existingPublication = instanceIdToPublicationMap.get(newClient.getInstanceId());
                    if(existingPublication != null && existingClient != null &&
                            existingClient.getIncomingStreamId() == newClient.getIncomingStreamId() &&
                            existingClient.getIncomingChannel().equals(newClient.getIncomingChannel()))
                    {
                        LOGGER.debug("Using existing publication for client instance={} with channel={} and stream={}",
                                existingClient.getInstanceId(), existingClient.getIncomingChannel(), existingClient.getIncomingStreamId());
                    }
                    else
                    {
                        //An existing publication for this instance with a different channel and stream needs to be closed
                        if(existingPublication != null)
                        {
                            existingPublication.close();
                        }
                        Publication publication = aeron.addPublication(newClient.getIncomingChannel(), newClient.getIncomingStreamId());
                        instanceIdToPublicationMap.put(newClient.getInstanceId(), publication);
                    }

                    //TODO(JAF): Switch to a collection to enforce unique values
                    List<Long> updatedKeys = new ArrayList<Long>();

                    //TODO(JAF): Make the diff code more efficient


                    //Remove existingClient from interest lists
                    if(existingClient != null)
                    {
                        AtomicArray<String> existingListenKeys = existingClient.getListenKeys();
                        for (int i = 0; i < existingListenKeys.size(); i++)
                        {
                            long hash = existingListenKeys.get(i).hashCode();
                            removeClientListenKey(existingClient, hash);
                        }

                        AtomicArray<DiscoverableTransport> existingValues = existingClient.getValues();
                        for (int i = 0; i < existingValues.size(); i++)
                        {
                            Discoverable existingValue = existingValues.get(i);
                            long hash = existingValue.getKey().hashCode();
                            removeClientValue(existingClient, hash, updatedKeys);
                        }
                    }

                    //Add newClient to interest lists
                    AtomicArray<String> newListenKeys = newClient.getListenKeys();
                    for(int i = 0; i < newListenKeys.size(); i++)
                    {
                        long hash = newListenKeys.get(i).hashCode();
                        addClientListenKey(newClient, hash);
                    }

                    AtomicArray<DiscoverableTransport> newValues = newClient.getValues();
                    for(int i = 0; i < newValues.size(); i++)
                    {
                        Discoverable newValue = newValues.get(i);
                        long hash = newValue.getKey().hashCode();
                        addClientValue(newClient, hash, updatedKeys);
                    }

                    notifyListeners(updatedKeys, newClient);
                    notifyNewClient(newClient);
                }
            }
            else
            {
                //TODO(JAF): Log heartbeats if needed
            }
        }
    };
}
