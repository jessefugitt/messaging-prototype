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
package org.kaazing.messaging.example;

import org.kaazing.messaging.common.discovery.service.discoverabletransport.dynamic.DynamicConsumerDiscoveryService;
import org.kaazing.messaging.common.discovery.service.discoverabletransport.file.PropertiesConfiguredDiscoveryService;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.client.MessageProducer;
import org.kaazing.messaging.common.destination.Topic;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.transport.DiscoverableTransport;
import org.kaazing.messaging.driver.MessagingDriver;

import java.io.IOException;

public class TopicProducerExample
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        MessagingDriver driver = new MessagingDriver();
        DiscoveryService<DiscoverableTransport> discoveryService = new DynamicConsumerDiscoveryService("aeron:udp?remote=127.0.0.1:40001");
        //DiscoveryService<DiscoverableTransport> discoveryService = new ZooKeeperDiscoveryService("127.0.0.1:2181");
        //DiscoveryService<DiscoverableTransport> discoveryService = new PropertiesConfiguredDiscoveryService("topics.properties");
        driver.setDiscoveryService(discoveryService);
        discoveryService.start();

        Topic topic = new Topic("STOCKS.ABC");

        MessageProducer messageProducer = new MessageProducer(driver, topic);


        for(int i = 0; i < 5; i++)
        {
            Message message = new Message(1024);
            message.getUnsafeBuffer().putInt(0, 567);
            message.setBufferOffset(0);
            message.setBufferLength(4);

            boolean result = messageProducer.offer(message);
            System.out.println("Sent message with result: " + result);

            Thread.sleep(10000);
        }
        messageProducer.close();
        driver.close();
    }
}
