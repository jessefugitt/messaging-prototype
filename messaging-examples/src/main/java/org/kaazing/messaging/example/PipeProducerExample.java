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
import org.kaazing.messaging.common.transport.aeron.AeronTransportContext;

public class PipeProducerExample
{
    public static void main(String[] args)
    {
        BaseTransportContext context = new AeronTransportContext();

        Pipe pipe = new Pipe("aeron:udp?remote=127.0.0.1:40124", 10);

        MessageProducer messageProducer = new MessageProducer(context, pipe);

        Message message = new Message(1024);
        message.getUnsafeBuffer().putInt(0, 567);
        message.setBufferOffset(0);
        message.setBufferLength(4);

        for(int i = 0; i < 10000000; )
        {
            boolean result = messageProducer.offer(message);
            if(result)
            {
                if((i + 1) % 100000 == 0)
                {
                    System.out.println("Sent message " + i + " with result: " + result);
                }
                i++;
            }
            //System.out.println("Sent message with result: " + result);
        }

        messageProducer.close();
        context.close();
    }
}
