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
package org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic;

import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.transport.DiscoverableTransport;
import org.kaazing.messaging.common.transport.AtomicArrayWithArg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.collections.MutableInteger;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class DynamicConsumerDiscoveryService implements DiscoveryService<DiscoverableTransport>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConsumerDiscoveryService.class);

    private final DiscoveryClientContext discoveryClientContext;
    //TODO(JAF): Refactor to avoid using ConcurrentHashMap
    private final Map<String, AtomicArrayWithArg<Consumer<DiscoveryEvent<DiscoverableTransport>>, DiscoveryEvent<DiscoverableTransport>>> listenerMap = new ConcurrentHashMap<>();
    //TODO(JAF): Update keyToClientsMap as clients are added, updated, and removed and refactor to avoid using ConcurrentHashMap
    private final Map<String, AtomicArrayWithArg<Client, String>> keyToClientsMap = new ConcurrentHashMap<>();
    private ThreadLocal<DiscoveryEvent<DiscoverableTransport>> tlDiscoveryEvent = new ThreadLocal<>().withInitial(() -> new DiscoveryEvent<DiscoverableTransport>());

    public DynamicConsumerDiscoveryService(String outgoingChannel)
    {
        discoveryClientContext = new DiscoveryClientContext(outgoingChannel);
    }

    @Override
    public AtomicArray<DiscoverableTransport> getValues(String key) {
        final AtomicArray<DiscoverableTransport> matchingTransports = new AtomicArray<>();
        AtomicArrayWithArg<Client, String> matchingClients = keyToClientsMap.get(key);
        if(matchingClients != null)
        {
            //TODO(JAF): Refactor this to not capture
            long totalMatches = matchingClients.doActionWithArg(0, (client, keyToMatch) ->
                    {
                        final MutableInteger perClientTotal = new MutableInteger();
                        AtomicArray<DiscoverableTransport> clientTransports = client.getValues();

                        //TODO(JAF): Refactor this to not capture
                        clientTransports.forEach((discoverableTransport) ->
                        {
                            if (keyToMatch.equals(discoverableTransport.getKey())) {
                                matchingTransports.add(discoverableTransport);
                                perClientTotal.value++;
                            }
                        });
                        return perClientTotal.value;
                    },
                    key);
        }
        return matchingTransports;
    }

    @Override
    public void addDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener) {
        Collection<Consumer<DiscoveryEvent<DiscoverableTransport>>> listeners = listenerMap.computeIfAbsent(key, (ignore) -> new AtomicArrayWithArg<>());
        if(!listeners.contains(listener)) {
            listeners.add(listener);
        }
        discoveryClientContext.getClient().addListenKey(key);
    }

    @Override
    public void removeDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener) {
        Collection<Consumer<DiscoveryEvent<DiscoverableTransport>>> listeners = listenerMap.get(key);
        if(listeners != null) {
            listeners.remove(listener);
        }
        discoveryClientContext.getClient().removeListenKey(key);
    }

    @Override
    public void registerValue(String key, DiscoverableTransport value) {
        discoveryClientContext.getClient().addValue(value);
    }

    @Override
    public void unregisterValue(String key, DiscoverableTransport value) {
        discoveryClientContext.getClient().removeValue(value);
    }

    @Override
    public DynamicConsumerDiscoveryService start() {
        discoveryClientContext.setClientDiscoveryListener(clientDiscoveryListener);
        discoveryClientContext.start();
        return this;
    }

    @Override
    public DynamicConsumerDiscoveryService stop() {
        discoveryClientContext.unsetClientDiscoveryListener();
        discoveryClientContext.stop();
        return this;
    }

    private final Consumer<ClientDiscoveryEvent> clientDiscoveryListener = new Consumer<ClientDiscoveryEvent>() {
        @Override
        public void accept(ClientDiscoveryEvent clientDiscoveryEvent) {
            if(clientDiscoveryEvent.getExistingClientInfo() == null)
            {

                AtomicArray<DiscoverableTransport> values = clientDiscoveryEvent.getNewClientInfo().getValues();
                for(int i = 0; i < values.size(); i++)
                {
                    DiscoverableTransport value =  values.get(i);
                    AtomicArrayWithArg<Client, String> matchingClients = keyToClientsMap.computeIfAbsent(value.getKey(),
                            (ignore) -> new AtomicArrayWithArg<Client, String>());
                    if(!matchingClients.contains(clientDiscoveryEvent.getNewClientInfo()))
                    {
                        matchingClients.add(clientDiscoveryEvent.getNewClientInfo());
                    }
                    LOGGER.debug("Discovered new remote client that has a transport key={}, physical address={}", value.getKey(), value.getTransportHandle().getPhysicalAddress());

                    AtomicArrayWithArg<Consumer<DiscoveryEvent<DiscoverableTransport>>, DiscoveryEvent<DiscoverableTransport>> matchingListeners = listenerMap.get(value.getKey());
                    if(matchingListeners != null)
                    {
                        DiscoveryEvent<DiscoverableTransport> interestListChangedEvent = tlDiscoveryEvent.get();
                        interestListChangedEvent.clear();
                        interestListChangedEvent.getAdded().add(value);
                        matchingListeners.doActionWithArg(0,
                                (consumer, event) ->
                                {
                                    consumer.accept(event);
                                    return 0;
                                },
                                interestListChangedEvent);
                    }
                }
            }
            else
            {
                //TODO(JAF): Added support for updates and removes

                AtomicArray<DiscoverableTransport> values = clientDiscoveryEvent.getNewClientInfo().getValues();
                for(int i = 0; i < values.size(); i++)
                {
                    DiscoverableTransport value =  values.get(i);
                    AtomicArrayWithArg<Client, String> matchingClients = keyToClientsMap.computeIfAbsent(value.getKey(),
                            (ignore) -> new AtomicArrayWithArg<Client, String>());
                    if(!matchingClients.contains(clientDiscoveryEvent.getNewClientInfo()))
                    {
                        matchingClients.add(clientDiscoveryEvent.getNewClientInfo());
                    }
                    boolean alreadyExists = false;
                    AtomicArray<DiscoverableTransport> existingValues = clientDiscoveryEvent.getExistingClientInfo().getValues();
                    for(int j = 0; j < existingValues.size() && alreadyExists == false; j++)
                    {
                        String existingValueId = existingValues.get(j).getTransportHandle().getId();
                        if(value.getTransportHandle().getId().equals(existingValueId))
                        {
                            alreadyExists = true;
                        }
                    }

                    LOGGER.debug("Discovered existing remote client that has a transport with key={}, physical address={}", value.getKey(), value.getTransportHandle().getPhysicalAddress());

                    if(alreadyExists == false) {

                        AtomicArrayWithArg<Consumer<DiscoveryEvent<DiscoverableTransport>>, DiscoveryEvent<DiscoverableTransport>> matchingListeners = listenerMap.get(value.getKey());
                        if (matchingListeners != null) {
                            DiscoveryEvent<DiscoverableTransport> interestListChangedEvent = tlDiscoveryEvent.get();
                            interestListChangedEvent.clear();
                            interestListChangedEvent.getAdded().add(value);
                            matchingListeners.doActionWithArg(0,
                                    (consumer, event) ->
                                    {
                                        consumer.accept(event);
                                        return 0;
                                    },
                                    interestListChangedEvent);
                        }
                    }
                }
            }
        }
    };
}
