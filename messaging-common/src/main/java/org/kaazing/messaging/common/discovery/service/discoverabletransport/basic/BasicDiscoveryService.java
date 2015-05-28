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
package org.kaazing.messaging.common.discovery.service.discoverabletransport.basic;


import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.discovery.DiscoverableTransport;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class BasicDiscoveryService implements DiscoveryService<DiscoverableTransport>
{
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<String, AtomicArray<DiscoverableTransport>> values = new ConcurrentHashMap<>();

    @Override
    public AtomicArray<DiscoverableTransport> getValues(String key)
    {
        return values.get(key);
    }

    @Override
    public void addDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener)
    {
        //Do nothing since there are no dynamic change events
    }

    @Override
    public void removeDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener)
    {
        //Do nothing since there are no dynamic change events
    }

    @Override
    public void registerValue(String key, DiscoverableTransport value)
    {
        readWriteLock.writeLock().lock();
        try
        {
            AtomicArray<DiscoverableTransport> matches = values.computeIfAbsent(key,
                    (ignore) -> new AtomicArray<DiscoverableTransport>());
            if(!matches.contains(value)) {
                matches.add(value);
            }
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void unregisterValue(String key, DiscoverableTransport value)
    {
        readWriteLock.writeLock().lock();
        try
        {
            AtomicArray<DiscoverableTransport> matches = values.get(key);
            matches.removeIf((element) -> element.getTransportHandle().getId().equals(value.getTransportHandle().getId()));
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public BasicDiscoveryService start()
    {
        return this;
    }

    @Override
    public BasicDiscoveryService stop()
    {
        return this;
    }
}
