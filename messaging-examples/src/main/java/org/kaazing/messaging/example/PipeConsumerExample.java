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
import org.kaazing.messaging.client.MessageConsumer;
import org.kaazing.messaging.client.destination.Pipe;
import org.kaazing.messaging.driver.MessagingDriver;

import java.util.function.Consumer;

public class PipeConsumerExample
{
    public static int message2Ctr = 0;
    public static int message3Ctr = 0;
    public static void main(String[] args)
    {
        Pipe pipe = new Pipe("aeron:udp?remote=127.0.0.1:40124|streamId=10");

        MessageConsumer messageConsumer1 = new MessageConsumer(pipe, new Consumer<Message>() {

            @Override
            public void accept(Message message)
            {
                System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()));
            }
        });

        MessageConsumer messageConsumer2 = new MessageConsumer(pipe,
                //message -> System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
                message ->
                {
                    message2Ctr += 1;//message.getBuffer().getInt(message.getBufferOffset());
                    if((message2Ctr + 1) % 100000 == 0)
                    {
                        System.out.println("(1)Received " + message2Ctr + " messages so far");
                    }
                }
        );

        MessageConsumer messageConsumer3 = new MessageConsumer(pipe,
                //message -> System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
                message ->
                {
                    message3Ctr += 1;//message.getBuffer().getInt(message.getBufferOffset());
                    if((message3Ctr + 1) % 100000 == 0)
                    {
                        System.out.println("(2)Received " + message3Ctr + " messages so far");
                    }
                }
        );


        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        messageConsumer1.close();
        messageConsumer2.close();
        messageConsumer3.close();
        MessagingDriver.getInstance().close();
    }
}
