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
import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToLongBiFunction;

public class MessageProducer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private final MessageFlow messageFlow;
    private AtomicArrayWithArg<SendingTransport, Message> transports = new AtomicArrayWithArg<SendingTransport, Message>();
    private final TransportContext context;

    public MessageProducer(BaseTransportContext context, MessageFlow messageFlow)
    {
        this.messageFlow = messageFlow;
        this.context = context;
        createSendingTransports(messageFlow);

        TransportCommand transportCommand = new TransportCommand();
        transportCommand.setCommandCompletedAction(commandCompletedAction);
        transportCommand.setType(TransportCommand.TYPE_ADD_DISCOVERY_LISTENER);
        transportCommand.setDiscoveryKey(messageFlow.getLogicalName());
        transportCommand.setDiscoveredTransportsAction(discoveredTransportsAction);
        context.enqueueCommand(transportCommand);
    }

    protected void createSendingTransports(MessageFlow messageFlow)
    {
        List<SendingTransport> sendingTransports = context.createSendingTransports(messageFlow);
        if(sendingTransports != null)
        {
            for(int i = 0; i < sendingTransports.size(); i++) {
                TransportCommand transportCommand = new TransportCommand();
                transportCommand.setCommandCompletedAction(commandCompletedAction);
                transportCommand.setType(TransportCommand.TYPE_ADD_SENDING_TRANSPORT);
                transportCommand.setSendingTransport(sendingTransports.get(i));
                context.enqueueCommand(transportCommand);
            }
        }
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
        transports.doActionWithArg(0, submitAction, message);
    }

    /**
     * Non-blocking call to submit a message
     * @param message
     * @return true if successful or false otherwise
     */
    public boolean offer(Message message)
    {
        return transports.doActionWithArgToBoolean(0, offerAction, message);
    }


    /**
     * Removes all sending transports from the message producer and closes them
     */
    public void close()
    {
        TransportCommand transportCommand1 = new TransportCommand();
        transportCommand1.setCommandCompletedAction(commandCompletedAction);
        transportCommand1.setType(TransportCommand.TYPE_REMOVE_DISCOVERY_LISTENER);
        transportCommand1.setDiscoveryKey(messageFlow.getLogicalName());
        transportCommand1.setDiscoveredTransportsAction(discoveredTransportsAction);
        context.enqueueCommand(transportCommand1);

        transports.forEach((sendingTransport) ->
        {
            TransportCommand transportCommand = new TransportCommand();
            transportCommand.setCommandCompletedAction(commandCompletedAction);
            transportCommand.setType(TransportCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT);
            transportCommand.setSendingTransport(sendingTransport);
            context.enqueueCommand(transportCommand);
        });
    }

    private final ToLongBiFunction<SendingTransport, Message> submitAction = new ToLongBiFunction<SendingTransport, Message>()
    {
        @Override
        public long applyAsLong(SendingTransport sendingTransport, Message message)
        {
            sendingTransport.submit(message);
            return 0;
        }
    };

    private final ToLongBiFunction<SendingTransport, Message> offerAction = new ToLongBiFunction<SendingTransport, Message>()
    {
        @Override
        public long applyAsLong(SendingTransport sendingTransport, Message message)
        {
            return sendingTransport.offer(message);
        }
    };

    private final Consumer<TransportCommand> commandCompletedAction = new Consumer<TransportCommand>() {
        @Override
        public void accept(TransportCommand transportCommand) {
            if(transportCommand.getType() == TransportCommand.TYPE_ADD_SENDING_TRANSPORT && transportCommand.getSendingTransport() != null) {
                transports.add(transportCommand.getSendingTransport());
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT && transportCommand.getSendingTransport() != null)
            {
                transports.remove(transportCommand.getSendingTransport());
                transportCommand.getSendingTransport().close();
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_ADD_DISCOVERY_LISTENER ||
                    transportCommand.getType() == TransportCommand.TYPE_REMOVE_DISCOVERY_LISTENER)
            {
                LOGGER.debug("Message producer command completed with type={}", transportCommand.getType());
            }
            else
            {
                LOGGER.warn("Unexpected transport command with type={} in MessageProducer completed action", transportCommand.getType());
            }
        }
    };


    private final Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction = new Consumer<DiscoveryEvent<DiscoverableTransport>>()
    {
        @Override
        public void accept(DiscoveryEvent<DiscoverableTransport> event)
        {
            List<DiscoverableTransport> added = event.getAdded();
            List<DiscoverableTransport> removed = event.getRemoved();

            //Handle updated later if necessary
            List<DiscoverableTransport> updated = event.getUpdated();


            if(added != null)
            {
                for(int i = 0; i < added.size(); i++)
                {
                    TransportHandle transportHandle = added.get(i).getTransportHandle();
                    if(transportHandle != null)
                    {
                        //TODO(JAF): Refactor to avoid capture
                        final String targetTransportHandleId = transportHandle.getId();
                        SendingTransport match = transports.findFirst(
                                (sendingTransport) -> targetTransportHandleId.equals(sendingTransport.getTargetTransportHandleId())
                        );

                        if(match == null)
                        {
                            SendingTransport sendingTransport = context.createSendingTransportFromHandle(transportHandle);
                            TransportCommand transportCommand = new TransportCommand();
                            transportCommand.setCommandCompletedAction(commandCompletedAction);
                            transportCommand.setType(TransportCommand.TYPE_ADD_SENDING_TRANSPORT);
                            transportCommand.setSendingTransport(sendingTransport);
                            context.enqueueCommand(transportCommand);
                        }
                    }
                }
            }

            if(removed != null)
            {
                for(int i = 0; i < removed.size(); i++)
                {
                    TransportHandle transportHandle = removed.get(i).getTransportHandle();
                    if(transportHandle != null)
                    {
                        //TODO(JAF): Refactor to avoid capture
                        final String targetTransportHandleId = transportHandle.getId();
                        if(targetTransportHandleId != null)
                        {
                            transports.forEach((sendingTransport) ->
                            {
                                if(targetTransportHandleId.equals(sendingTransport.getTargetTransportHandleId())) {
                                    TransportCommand transportCommand = new TransportCommand();
                                    transportCommand.setCommandCompletedAction(commandCompletedAction);
                                    transportCommand.setType(TransportCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT);
                                    transportCommand.setSendingTransport(sendingTransport);
                                    context.enqueueCommand(transportCommand);
                                }
                            });
                        }
                    }
                }
            }
        }
    };
}
