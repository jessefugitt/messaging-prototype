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
package org.kaazing.discovery.messaging.client;

import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.Client;
import org.kaazing.messaging.common.destination.Topic;
import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.DiscoveryConfiguration;
import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.DiscoveryClientContext;
import org.kaazing.messaging.common.transport.DiscoverableTransport;
import org.kaazing.messaging.common.transport.TransportHandle;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;

import java.util.UUID;

public class ClientRunner
{
    private static final boolean EMBEDDED_MEDIA_DRIVER = DiscoveryConfiguration.EMBEDDED_MEDIA_DRIVER;
    public static void main(String[] args) throws InterruptedException
    {
        MediaDriver driver = null;
        if (EMBEDDED_MEDIA_DRIVER)
        {
            MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
            mediaDriverContext.dirsDeleteOnExit(true);
            driver = MediaDriver.launchEmbedded(mediaDriverContext);
        }

        DiscoveryClientContext discoveryClientContext = new DiscoveryClientContext("aeron:udp?remote=127.0.0.1:40001");
        //TODO(JAF): Need to pass the media driver dir to the aeron context if in embedded mode
        Client client = discoveryClientContext.getClient();
        Topic topic1 = new Topic("STOCKS.INTC");
        DiscoverableTransport discoverableTransport1 = new DiscoverableTransport(topic1.getLogicalName(),
                new TransportHandle("aeron:udp?remote=127.0.0.1:40123|streamId=10", TransportHandle.Type.Aeron, UUID.randomUUID().toString()));


        Topic topic2 = new Topic("STOCKS.GOOG");
        DiscoverableTransport discoverableTransport2 = new DiscoverableTransport(topic2.getLogicalName(),
                new TransportHandle("aeron:udp?remote=127.0.0.1:40123|streamId=11", TransportHandle.Type.Aeron, UUID.randomUUID().toString()));

        client.addListenKey("STOCKS.INTC");
        client.addListenKey("STOCKS.GOOG");
        client.addValue(discoverableTransport2);

        discoveryClientContext.start();
        Thread.sleep(10000);
        discoveryClientContext.stop();
        CloseHelper.quietClose(driver);
    }
}
