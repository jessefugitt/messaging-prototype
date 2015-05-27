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

import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.transport.ReceivingTransport;
import org.kaazing.messaging.common.command.MessagingCommand;
import org.kaazing.messaging.common.transport.TransportContext;
import org.kaazing.messaging.driver.MessagingDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.List;
import java.util.function.Consumer;

public class MessageConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    private final MessagingDriver messagingDriver;
    private final MessageFlow messageFlow;
    private long messageConsumerId;
    private final Consumer<Message> messageHandler;

    public MessageConsumer(MessagingDriver messagingDriver, MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        this.messagingDriver = messagingDriver;
        this.messageFlow = messageFlow;
        this.messageHandler = messageHandler;

        MessagingCommand messagingCommand = new MessagingCommand();
        messagingCommand.setCommandCompletedAction(commandCompletedAction);
        messagingCommand.setType(MessagingCommand.TYPE_CREATE_CONSUMER);
        messagingCommand.setMessageFlow(messageFlow);
        messagingCommand.setMessageHandler(messageHandler);

        messagingDriver.enqueueCommand(messagingCommand);
    }

    public MessageFlow getMessageFlow()
    {
        return messageFlow;
    }

    /**
     * Removes all receiving transports from the message consumerand closes them
     */
    public void close()
    {
        MessagingCommand messagingCommand = new MessagingCommand();
        messagingCommand.setType(MessagingCommand.TYPE_DELETE_CONSUMER);
        messagingCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueCommand(messagingCommand);
    }

    private final Consumer<MessagingCommand> commandCompletedAction = new Consumer<MessagingCommand>() {
        @Override
        public void accept(MessagingCommand messagingCommand) {

            if(messagingCommand.getType() == MessagingCommand.TYPE_CREATE_CONSUMER)
            {
                messageConsumerId = messagingCommand.getMessageConsumerId();
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageConsumer completed action", messagingCommand.getType());
            }
        }
    };
}
