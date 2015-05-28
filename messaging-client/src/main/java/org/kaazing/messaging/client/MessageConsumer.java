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

import org.kaazing.messaging.common.command.ClientCommand;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.driver.MessagingDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class MessageConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    private final MessagingDriver messagingDriver;
    private final MessageFlow messageFlow;
    private long messageConsumerId;
    private final Consumer<Message> messageHandler;

    public MessageConsumer(MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        this(MessagingDriver.getInstance(), messageFlow, messageHandler);
    }

    public MessageConsumer(MessagingDriver messagingDriver, MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        this.messagingDriver = messagingDriver;
        this.messageFlow = messageFlow;
        this.messageHandler = messageHandler;

        ClientCommand clientCommand = new ClientCommand(ClientCommand.TYPE_CREATE_CONSUMER);
        clientCommand.setCommandCompletedAction(commandCompletedAction);
        clientCommand.setMessageFlow(messageFlow);
        clientCommand.setMessageHandler(messageHandler);
        messagingDriver.enqueueClientCommand(clientCommand);
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
        ClientCommand clientCommand = new ClientCommand(ClientCommand.TYPE_DELETE_CONSUMER);
        clientCommand.setMessageFlow(messageFlow);
        messagingDriver.enqueueClientCommand(clientCommand);
    }

    private final Consumer<ClientCommand> commandCompletedAction = new Consumer<ClientCommand>() {
        @Override
        public void accept(ClientCommand clientCommand) {

            if(clientCommand.getType() == ClientCommand.TYPE_CREATE_CONSUMER)
            {
                messageConsumerId = clientCommand.getMessageConsumerId();
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageConsumer completed action", clientCommand.getType());
            }
        }
    };
}
