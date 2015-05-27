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

import org.kaazing.messaging.common.command.MessagingCommand;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.transport.*;
import org.kaazing.messaging.driver.MessagingDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToLongBiFunction;

public class MessageProducer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private final MessagingDriver messagingDriver;
    private final MessageFlow messageFlow;
    private OneToOneConcurrentArrayQueue<Message> sendQueue;
    private OneToOneConcurrentArrayQueue<Message> freeQueue;
    private int messageProducerIndex = -1;
    private long messageProducerId;

    public MessageProducer(MessagingDriver messagingDriver, MessageFlow messageFlow)
    {
        this.messageFlow = messageFlow;
        this.messagingDriver = messagingDriver;

        MessagingCommand messagingCommand = new MessagingCommand();
        messagingCommand.setCommandCompletedAction(commandCompletedAction);
        messagingCommand.setType(MessagingCommand.TYPE_CREATE_PRODUCER);
        messagingCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueCommand(messagingCommand);
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
        throw new UnsupportedOperationException("Blocking submit call not currently supported");
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
            Message sendMessage = freeQueue.poll();
            if(sendMessage != null)
            {
                if(sendMessage.getBuffer().capacity() < message.getBufferLength())
                {
                    //TODO(JAF): Clean this up to not have to GC direct buffers
                    UnsafeBuffer newBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(message.getBufferLength()));
                    sendMessage.setBuffer(newBuffer);
                }

                sendMessage.setBufferLength(message.getBufferLength());
                sendMessage.setBufferOffset(message.getBufferOffset());
                sendMessage.getUnsafeBuffer().putBytes(message.getBufferOffset(), message.getBuffer(), message.getBufferOffset(), message.getBufferLength());

                message.setMessageProducerIndex(messageProducerIndex);
                result = sendQueue.offer(sendMessage);
            }
        }
        return result;
    }


    /**
     * Removes all sending transports from the message producer and closes them
     */
    public void close()
    {
        MessagingCommand messagingCommand = new MessagingCommand();
        messagingCommand.setMessageProducerIndex(messageProducerIndex);
        messagingCommand.setType(MessagingCommand.TYPE_DELETE_PRODUCER);
        messagingCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueCommand(messagingCommand);

    }


    private final Consumer<MessagingCommand> commandCompletedAction = new Consumer<MessagingCommand>() {
        @Override
        public void accept(MessagingCommand messagingCommand) {
            if(messagingCommand.getType() == MessagingCommand.TYPE_CREATE_PRODUCER)
            {
                messageProducerIndex = messagingCommand.getMessageProducerIndex();
                messageProducerId = messagingCommand.getMessageProducerId();
                freeQueue = messagingCommand.getFreeQueue();
                sendQueue = messagingCommand.getSendQueue();
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageProducer completed action", messagingCommand.getType());
            }
        }
    };

}
