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
package org.kaazing.messaging.client;

import org.kaazing.messaging.client.message.Message;
import org.kaazing.messaging.driver.command.ClientCommand;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.MessagingDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MessageProducer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private final MessagingDriver messagingDriver;
    private final MessageFlow messageFlow;
    private OneToOneConcurrentArrayQueue<DriverMessage> sendQueue;
    private OneToOneConcurrentArrayQueue<DriverMessage> freeQueue;
    private int messageProducerIndex = -1;
    private long messageProducerId;
    private final IdleStrategy submitIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    public MessageProducer(MessageFlow messageFlow)
    {
        this(MessagingDriver.getInstance(), messageFlow);
    }

    public MessageProducer(MessagingDriver messagingDriver, MessageFlow messageFlow)
    {
        this.messageFlow = messageFlow;
        this.messagingDriver = messagingDriver;

        ClientCommand clientCommand = new ClientCommand(ClientCommand.TYPE_CREATE_PRODUCER);
        clientCommand.setCommandCompletedAction(commandCompletedAction);
        clientCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueClientCommand(clientCommand);
    }



    public MessageFlow getMessageFlow()
    {
        return messageFlow;
    }

    /**
     * Blocking call to submit a message
     * @param message
     */
    public void submit(Message message)
    {
        boolean result = offer(message);
        while(result == false)
        {
            submitIdleStrategy.idle(0);
            result = offer(message);
        }
    }

    /**
     * Non-blocking call to submit a message
     * @param message
     * @return true if successful or false otherwise
     */
    public boolean offer(Message message)
    {
        boolean result = false;
        if(sendQueue != null) {
            DriverMessage sendDriverMessage = freeQueue.poll();
            if(sendDriverMessage != null)
            {
                if(sendDriverMessage.getBuffer().capacity() < message.getBufferLength())
                {
                    //TODO(JAF): Clean this up to not have to GC direct buffers
                    UnsafeBuffer newBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(message.getBufferLength()));
                    sendDriverMessage.setBuffer(newBuffer);
                }

                sendDriverMessage.setBufferLength(message.getBufferLength());
                sendDriverMessage.setBufferOffset(message.getBufferOffset());
                sendDriverMessage.getUnsafeBuffer().putBytes(message.getBufferOffset(), message.getBuffer(), message.getBufferOffset(), message.getBufferLength());

                sendDriverMessage.setMessageProducerIndex(messageProducerIndex);
                result = sendQueue.offer(sendDriverMessage);
            }
        }
        return result;
    }


    /**
     * Removes all sending transports from the message producer and closes them
     */
    public void close()
    {
        ClientCommand messagingCommand = new ClientCommand(ClientCommand.TYPE_DELETE_PRODUCER);
        messagingCommand.setMessageProducerIndex(messageProducerIndex);
        messagingCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueClientCommand(messagingCommand);
    }


    private final Consumer<ClientCommand> commandCompletedAction = new Consumer<ClientCommand>() {
        @Override
        public void accept(ClientCommand clientCommand) {
            if(clientCommand.getType() == ClientCommand.TYPE_CREATE_PRODUCER)
            {
                messageProducerIndex = clientCommand.getMessageProducerIndex();
                messageProducerId = clientCommand.getMessageProducerId();
                freeQueue = clientCommand.getFreeQueue();
                sendQueue = clientCommand.getSendQueue();
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageProducer completed action", clientCommand.getType());
            }
        }
    };

}
