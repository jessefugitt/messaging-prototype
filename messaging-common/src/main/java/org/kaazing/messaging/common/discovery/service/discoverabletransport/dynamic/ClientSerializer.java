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

import org.kaazing.messaging.common.transport.DiscoverableTransport;
import uk.co.real_logic.agrona.collections.MutableInteger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

public class ClientSerializer
{
    protected int putDiscoverableTransport(UnsafeBuffer buffer, int index, DiscoverableTransport discoverableTransport)
    {
        int topicSizeIndex = index;
        index += 4;

        String logicalName = discoverableTransport.getKey();
        byte[] logicalNameBytes = logicalName.getBytes(StandardCharsets.UTF_8);
        String physicalAddress = discoverableTransport.getTransportHandle().getPhysicalAddress();
        byte[] physicalAddressBytes = physicalAddress.getBytes(StandardCharsets.UTF_8);
        String transportHandleId = discoverableTransport.getTransportHandle().getId();
        byte[] transportHandleIdBytes = transportHandleId.getBytes(StandardCharsets.UTF_8);

        buffer.putInt(index, logicalNameBytes.length);
        index += 4;
        buffer.putBytes(index, logicalNameBytes);
        index += logicalNameBytes.length;

        buffer.putInt(index, physicalAddressBytes.length);
        index += 4;
        buffer.putBytes(index, physicalAddressBytes);
        index += physicalAddressBytes.length;

        buffer.putInt(index, transportHandleIdBytes.length);
        index += 4;
        buffer.putBytes(index, transportHandleIdBytes);
        index += transportHandleIdBytes.length;

        //set the size
        buffer.putInt(topicSizeIndex, logicalNameBytes.length + 4 + physicalAddressBytes.length + 4 + transportHandleIdBytes.length + 4);
        return index;
    }
    public int serialize(final UnsafeBuffer buffer, Client client, byte contentType)
    {
        final MutableInteger index = new MutableInteger();
        index.value = 0;
        buffer.putByte(index.value, contentType);
        index.value += 1;

        //TODO(JAF): Reserve some space for compatibility versioning

        buffer.putLong(index.value, client.instanceId);
        index.value += 8;
        buffer.putLong(index.value, client.revision.get());
        index.value += 8;
        if(contentType == Client.FULL_TYPE)
        {
            String incomingChannel = client.getIncomingChannel();
            byte[] incomingChannelBytes = incomingChannel.getBytes(StandardCharsets.UTF_8);
            int incomingStreamId = client.getIncomingStreamId();

            buffer.putInt(index.value, incomingChannelBytes.length);
            index.value += 4;
            buffer.putBytes(index.value, incomingChannelBytes);
            index.value += incomingChannelBytes.length;
            buffer.putInt(index.value, incomingStreamId);
            index.value += 4;

            final int listenKeysSize = client.listenKeys.size();
            buffer.putInt(index.value, listenKeysSize);
            index.value += 4;

            client.listenKeys.forEach(
                    (listenKey) ->
                    {
                        int listenKeySizeIndex = index.value;
                        index.value += 4;

                        byte[] listenKeyBytes = listenKey.getBytes(StandardCharsets.UTF_8);

                        buffer.putInt(index.value, listenKeyBytes.length);
                        index.value += 4;
                        buffer.putBytes(index.value, listenKeyBytes);
                        index.value += listenKeyBytes.length;

                        buffer.putInt(listenKeySizeIndex, listenKeyBytes.length + 4);
                    }
            );

            final int valueListSize = client.values.size();
            buffer.putInt(index.value, valueListSize);
            index.value += 4;

            client.values.forEach(
                    (discoverableTransport) ->
                    {
                        index.value = putDiscoverableTransport(buffer, index.value, discoverableTransport);
                    }
            );
        }
        return index.value;
    }
}
