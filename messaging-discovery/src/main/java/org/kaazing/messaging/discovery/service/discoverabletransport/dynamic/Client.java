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

import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.discovery.DiscoverableTransport;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Client
{
    public static final byte FULL_TYPE = 0;
    public static final byte REVISION_ONLY_TYPE = 1;

    protected final byte contentType;
    protected final long instanceId;
    protected final AtomicLong revision = new AtomicLong(0);
    protected final String incomingChannel;
    protected final int incomingStreamId;
    protected final AtomicArray<String> listenKeys = new AtomicArray<>();
    protected final AtomicArray<DiscoverableTransport> values = new AtomicArray<>();

    public Client(String incomingChannel, int incomingStreamId)
    {
        this.instanceId = ThreadLocalRandom.current().nextLong();
        this.contentType = FULL_TYPE;
        this.incomingChannel = incomingChannel;
        this.incomingStreamId = incomingStreamId;
    }

    protected Client(long instanceId, byte contentType, String incomingChannel, int incomingStreamId)
    {
        this.instanceId = instanceId;
        this.contentType = contentType;
        this.incomingChannel = incomingChannel;
        this.incomingStreamId = incomingStreamId;
    }

    public byte getContentType()
    {
        return contentType;
    }

    public long getInstanceId()
    {
        return instanceId;
    }

    public long getRevision()
    {
        return revision.get();
    }

    public String getIncomingChannel()
    {
        return incomingChannel;
    }

    public int getIncomingStreamId()
    {
        return incomingStreamId;
    }

    public AtomicArray<String> getListenKeys()
    {
        return listenKeys;
    }

    public AtomicArray<DiscoverableTransport> getValues()
    {
        return values;
    }

    public void addListenKey(String key)
    {
        if(!listenKeys.contains(key))
        {
            listenKeys.add(key);
        }
        revision.incrementAndGet();
    }

    public void removeListenKey(String key)
    {
        listenKeys.remove(key);
        revision.incrementAndGet();
    }

    public void addValue(final DiscoverableTransport discoverable)
    {
        if(!values.contains(discoverable)) {
            values.add(discoverable);
        }
        revision.incrementAndGet();
    }

    public void removeValue(final DiscoverableTransport discoverable)
    {
        values.remove(discoverable);
        revision.incrementAndGet();
    }


}
