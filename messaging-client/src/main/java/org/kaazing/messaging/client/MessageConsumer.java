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
import org.kaazing.messaging.common.transport.TransportCommand;
import org.kaazing.messaging.common.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.List;
import java.util.function.Consumer;

public class MessageConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    private final TransportContext context;
    private final MessageFlow messageFlow;
    private final Consumer<Message> messageHandler;
    private AtomicArray<ReceivingTransport> transports = new AtomicArray<ReceivingTransport>();

    public MessageConsumer(TransportContext context, MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        this.context = context;
        this.messageFlow = messageFlow;
        this.messageHandler = messageHandler;
        createReceivingTransports(messageFlow, messageHandler);
    }

    public MessageFlow getMessageFlow()
    {
        return messageFlow;
    }

    protected void createReceivingTransports(MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        List<ReceivingTransport> receivingTransports = context.createReceivingTransports(messageFlow, messageHandler);
        if(receivingTransports != null)
        {
            for(int i = 0; i < receivingTransports.size(); i++) {
                TransportCommand transportCommand = new TransportCommand();
                transportCommand.setCommandCompletedAction(commandCompletedAction);
                transportCommand.setType(TransportCommand.TYPE_ADD_RECEIVING_TRANSPORT);
                transportCommand.setReceivingTransport(receivingTransports.get(i));
                context.enqueueCommand(transportCommand);
            }
        }
    }

    /**
     * Removes all receiving transports from the message consumerand closes them
     */
    public void close()
    {
        transports.forEach((receivingTransport) ->
        {
            TransportCommand transportCommand = new TransportCommand();
            transportCommand.setCommandCompletedAction(commandCompletedAction);
            transportCommand.setType(TransportCommand.TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT);
            transportCommand.setReceivingTransport(receivingTransport);
            context.enqueueCommand(transportCommand);
        });
    }

    private final Consumer<TransportCommand> commandCompletedAction = new Consumer<TransportCommand>() {
        @Override
        public void accept(TransportCommand transportCommand) {
            if(transportCommand.getType() == TransportCommand.TYPE_ADD_RECEIVING_TRANSPORT && transportCommand.getReceivingTransport() != null) {
                transports.add(transportCommand.getReceivingTransport());
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT && transportCommand.getReceivingTransport() != null)
            {
                transports.remove(transportCommand.getReceivingTransport());
                transportCommand.getReceivingTransport().close();
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageConsumer completed action", transportCommand.getType());
            }
        }
    };
}
