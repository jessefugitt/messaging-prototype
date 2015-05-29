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

import java.io.IOException;
import java.util.function.Consumer;

public class AmqpPipeConsumerExample
{
    public static void main(String[] args) throws IOException
    {
        MessagingDriver driver = new MessagingDriver();

        Pipe pipe = new Pipe("amqp://~127.0.0.1:5672");

        MessageConsumer messageConsumer1 = new MessageConsumer(driver, pipe, new Consumer<Message>() {

            @Override
            public void accept(Message message)
            {
                System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()));
            }
        });

        /*
        MessageConsumer messageConsumer2 = new MessageConsumer(context, pipe,
                message -> System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
        );
        */


        try
        {
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        messageConsumer1.close();
        driver.close();
    }
}
