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

import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.common.transport.TransportHandle;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.MutableInteger;

import java.nio.charset.StandardCharsets;

public class ClientDeserializer
{
    protected DiscoverableTransport getDiscoverableTransport(DirectBuffer buffer, int index)
    {
        int logicalNameBytesLength = buffer.getInt(index);
        index += 4;
        byte[] logicalNameBytes = new byte[logicalNameBytesLength];
        buffer.getBytes(index, logicalNameBytes);
        index += logicalNameBytesLength;

        int physicalAddressBytesLength = buffer.getInt(index);
        index += 4;
        byte[] physicalAddressBytes = new byte[physicalAddressBytesLength];
        buffer.getBytes(index, physicalAddressBytes);
        index += physicalAddressBytesLength;

        int transportHandleIdBytesLength = buffer.getInt(index);
        index += 4;
        byte[] transportHandleIdBytes = new byte[transportHandleIdBytesLength];
        buffer.getBytes(index, transportHandleIdBytes);
        index += transportHandleIdBytesLength;

        String logicalName = new String(logicalNameBytes, StandardCharsets.UTF_8);
        String physicalAddress = new String(physicalAddressBytes, StandardCharsets.UTF_8);
        String transportHandleId = new String(transportHandleIdBytes, StandardCharsets.UTF_8);

        //TODO(JAF): Add support for other schemes
        TransportHandle transportHandle = new TransportHandle(physicalAddress, "aeron", transportHandleId);
        DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);

        return discoverableTransport;
    }

    public Client deserialize(DirectBuffer buffer, int offset)
    {
        final MutableInteger index = new MutableInteger();
        index.value = offset;
        byte contentType = buffer.getByte(index.value);
        index.value += 1;

        //TODO(JAF): Reserve some space for compatibility versioning


        long instanceId = buffer.getLong(index.value);
        Client client = null;
        index.value += 8;
        long revision = buffer.getLong(index.value);
        index.value += 8;

        if(contentType == Client.FULL_TYPE)
        {
            int incomingChannelBytesLength = buffer.getInt(index.value);
            byte[] incomingChannelBytes = new byte[incomingChannelBytesLength];
            index.value += 4;
            buffer.getBytes(index.value, incomingChannelBytes);
            index.value += incomingChannelBytes.length;
            String incomingChannel = new String(incomingChannelBytes, StandardCharsets.UTF_8);

            int incomingStreamId = buffer.getInt(index.value);
            index.value += 4;

            client = new Client(instanceId, contentType, incomingChannel, incomingStreamId);
            final int listenKeysSize = buffer.getInt(index.value);
            index.value += 4;

            for (int i = 0; i < listenKeysSize; i++)
            {
                int valueLength = buffer.getInt(index.value);
                index.value += 4;
                int listenKeyBytesLength = buffer.getInt(index.value);
                index.value += 4;
                byte[] listenKeyBytes = new byte[listenKeyBytesLength];
                buffer.getBytes(index.value, listenKeyBytes);
                index.value += listenKeyBytesLength;
                String listenKey = new String(listenKeyBytes, StandardCharsets.UTF_8);
                client.addListenKey(listenKey);
                index.value += valueLength;
            }

            final int valueListSize = buffer.getInt(index.value);
            index.value += 4;

            for (int i = 0; i < valueListSize; i++)
            {
                int valueLength = buffer.getInt(index.value);
                index.value += 4;
                DiscoverableTransport discoverableTransport = getDiscoverableTransport(buffer, index.value);
                client.addValue(discoverableTransport);
                index.value += valueLength;
            }
        }
        else
        {
            client = new Client(instanceId, contentType, null, -1);
        }

        //Need to set the revision after deserializing producers and consumers which could bump the value
        client.revision.set(revision);
        return client;
    }
}
