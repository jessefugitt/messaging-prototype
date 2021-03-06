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

import org.kaazing.messaging.client.message.Message;
import org.kaazing.messaging.client.MessageProducer;
import org.kaazing.messaging.client.destination.Pipe;
import org.kaazing.messaging.driver.MessagingDriver;

public class PipeProducerExample
{
    public static void main(String[] args)
    {
        Pipe pipe = new Pipe("aeron:udp?remote=127.0.0.1:40124|streamId=10");
        //Pipe pipe = new Pipe("udp://127.0.0.1:40124");
        //Pipe pipe = new Pipe("tcp://127.0.0.1:40124");

        MessageProducer messageProducer = new MessageProducer(pipe);

        Message message = new Message(1024);
        message.getUnsafeBuffer().putInt(0, 567);
        message.setBufferOffset(0);
        message.setBufferLength(4);

        for(int i = 0; i < 11; )
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

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        messageProducer.close();
        MessagingDriver.getInstance().close();
    }
}
