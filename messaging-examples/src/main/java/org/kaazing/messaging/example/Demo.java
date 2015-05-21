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

import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.client.MessageConsumer;
import org.kaazing.messaging.client.MessageProducer;
import org.kaazing.messaging.common.destination.Topic;
import org.kaazing.messaging.common.transport.BaseTransportContext;
import org.kaazing.messaging.common.transport.aeron.AeronTransportContext;

import java.util.function.Consumer;

public class Demo
{
    public static void main(String[] args) {
        BaseTransportContext context = new AeronTransportContext();

        Topic topic = new Topic("STOCKS.ABC");

        MessageConsumer messageConsumer1 = new MessageConsumer(context, topic, new Consumer<Message>()
        {
            @Override
            public void accept(Message message)
            {
                System.out.println("Received message in anonymous callback");
            }
        });


        MessageConsumer messageConsumer2 = new MessageConsumer(context, topic,
                message -> System.out.println("Received message in lambda")
        );

        MessageProducer messageProducer = new MessageProducer(context, topic);

        Message message = new Message(1024);
        message.getUnsafeBuffer().putInt(0, 567);
        message.setBufferOffset(0);
        message.setBufferLength(4);

        messageProducer.offer(message);
        //messageProducer.submit(message);
    }
}
