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

import org.kaazing.messaging.discovery.service.discoverabletransport.dynamic.DynamicConsumerDiscoveryService;
import org.kaazing.messaging.client.message.Message;
import org.kaazing.messaging.client.MessageConsumer;
import org.kaazing.messaging.client.destination.Topic;
import org.kaazing.messaging.discovery.service.DiscoveryService;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.discovery.service.discoverabletransport.file.PropertiesConfiguredDiscoveryService;
import org.kaazing.messaging.driver.MessagingDriver;

import java.io.IOException;
import java.util.function.Consumer;

public class TopicConsumerExample
{

    public static void main(String[] args) throws IOException
    {
        MessagingDriver driver = new MessagingDriver();
        DiscoveryService<DiscoverableTransport> discoveryService = new DynamicConsumerDiscoveryService("aeron:udp?remote=127.0.0.1:40001");
        //DiscoveryService<DiscoverableTransport> discoveryService = new ZooKeeperDiscoveryService("127.0.0.1:2181");
        //DiscoveryService<DiscoverableTransport> discoveryService = new PropertiesConfiguredDiscoveryService("topics.properties");
        driver.setDiscoveryService(discoveryService);
        discoveryService.start();

        Topic topic = new Topic("STOCKS.ABC");

        MessageConsumer messageConsumer1 = new MessageConsumer(driver, topic, new Consumer<Message>() {

            @Override
            public void accept(Message message)
            {
                System.out.println("Consumer1 received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()));
            }
        });

        MessageConsumer messageConsumer2 = new MessageConsumer(driver, topic,
                message -> System.out.println("Consumer2 received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
        );

        try
        {
            Thread.sleep(60000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        messageConsumer1.close();
        messageConsumer2.close();
        driver.close();
    }
}
