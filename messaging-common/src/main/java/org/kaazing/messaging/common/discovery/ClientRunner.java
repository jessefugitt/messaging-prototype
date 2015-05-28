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
package org.kaazing.messaging.common.discovery;

import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.Client;
import org.kaazing.messaging.common.destination.Topic;
import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.DiscoveryClientContext;
import org.kaazing.messaging.common.transport.TransportHandle;

import java.util.UUID;

public class ClientRunner
{
    public static void main(String[] args) throws InterruptedException
    {
        DiscoveryClientContext discoveryClientContext = new DiscoveryClientContext("aeron:udp?remote=127.0.0.1:40001");
        //TODO(JAF): Need to pass the media driver dir to the aeron context if in embedded mode
        Client client = discoveryClientContext.getClient();
        Topic topic1 = new Topic("STOCKS.INTC");
        DiscoverableTransport discoverableTransport1 = new DiscoverableTransport(topic1.getLogicalName(),
                new TransportHandle("aeron:udp?remote=127.0.0.1:40123|streamId=10", "aeron", UUID.randomUUID().toString()));


        Topic topic2 = new Topic("STOCKS.GOOG");
        DiscoverableTransport discoverableTransport2 = new DiscoverableTransport(topic2.getLogicalName(),
                new TransportHandle("aeron:udp?remote=127.0.0.1:40123|streamId=11", "aeron", UUID.randomUUID().toString()));

        client.addListenKey("STOCKS.INTC");
        client.addListenKey("STOCKS.GOOG");
        client.addValue(discoverableTransport2);

        discoveryClientContext.start();
        Thread.sleep(10000);
        discoveryClientContext.stop();
    }
}
