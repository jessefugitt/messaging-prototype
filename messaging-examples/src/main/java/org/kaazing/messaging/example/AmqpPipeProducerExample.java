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
import org.kaazing.messaging.client.MessageProducer;
import org.kaazing.messaging.common.destination.Pipe;
import org.kaazing.messaging.common.transport.BaseTransportContext;
import org.kaazing.messaging.common.transport.amqp.AmqpProtonTransportContext;

import java.io.IOException;

public class AmqpPipeProducerExample
{
    public static void main(String[] args) throws IOException
    {
        BaseTransportContext context = new AmqpProtonTransportContext();

        Pipe pipe = new Pipe("amqp://127.0.0.1:5672");

        MessageProducer messageProducer = new MessageProducer(context, pipe);

        Message message = new Message(1024);
        message.getUnsafeBuffer().putInt(0, 567);
        message.setBufferOffset(0);
        message.setBufferLength(4);

        messageProducer.submit(message);
        System.out.println("Sent message");

        messageProducer.close();
        context.close();
    }
}