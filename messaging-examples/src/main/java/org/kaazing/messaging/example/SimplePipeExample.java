package org.kaazing.messaging.example;

import org.kaazing.messaging.client.MessageConsumer;
import org.kaazing.messaging.client.MessageProducer;
import org.kaazing.messaging.client.destination.Pipe;
import org.kaazing.messaging.client.message.Message;
import org.kaazing.messaging.driver.MessagingDriver;

public class SimplePipeExample
{
    public static void main(String[] args)
    {
        Pipe pipe = new Pipe("aeron:udp?remote=127.0.0.1:40124|streamId=10");

        MessagingDriver driver = new MessagingDriver();


        MessageConsumer messageConsumer = new MessageConsumer(driver, pipe,
                (message) -> System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
        );

        MessageProducer messageProducer = new MessageProducer(driver, pipe);

        Message message = new Message(1024);
        message.getUnsafeBuffer().putInt(0, 567);
        message.setBufferOffset(0);
        message.setBufferLength(4);


        for(int i = 0; i < 5; )
        {
            boolean result = messageProducer.offer(message);
            if(result)
            {
                i++;
                System.out.println("Sent message " + i);
            }
        }

        try {
            System.out.println("Exiting...");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        messageConsumer.close();
        messageProducer.close();
        driver.close();
        //MessagingDriver.getInstance().close();
    }
}
