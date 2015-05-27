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
package org.kaazing.messaging.driver;

import org.kaazing.messaging.common.command.MessagingCommand;
import org.kaazing.messaging.common.destination.Pipe;
import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.transport.*;
import org.kaazing.messaging.common.transport.aeron.AeronReceivingTransport;
import org.kaazing.messaging.common.transport.aeron.AeronSendingTransport;
import org.kaazing.messaging.common.transport.aeron.AeronTransportContext;
import org.kaazing.messaging.common.transport.amqp.AmqpProtonReceivingTransport;
import org.kaazing.messaging.common.transport.amqp.AmqpProtonSendingTransport;
import org.kaazing.messaging.common.transport.amqp.AmqpProtonTransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.common.uri.AeronUri;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class MessagingDriver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingDriver.class);

    private static final int COMMAND_QUEUE_DEFAULT_CAPACITY = 1024;
    //private static final int SEND_QUEUE_DEFAULT_CAPACITY = 4096;
    private static final int MESSAGE_PRODUCER_DEFAULT_CAPACITY = 512;
    private static final int MESSAGE_CONSUMER_DEFAULT_CAPACITY = 512;

    private final static int PER_PRODUCER_SEND_COUNT_LIMIT = 10;

    private final static int FRAGMENT_COUNT_LIMIT = 10;

    private DiscoveryService<DiscoverableTransport> discoveryService;
    //Single writer with lock free readers
    private final AtomicArray<ReceivingTransport> pollableReceivingTransports = new AtomicArray<ReceivingTransport>();

    private final MessageProducerMapping[] messageProducerMappings = new MessageProducerMapping[MESSAGE_PRODUCER_DEFAULT_CAPACITY];
    private Long2ObjectHashMap<AtomicArray<MessageProducerMapping>> logicalNameToProducerMap = new Long2ObjectHashMap<>();
    private final AtomicArray<OneToOneConcurrentArrayQueue<Message>> producerSendQueues = new AtomicArray<>();

    private final MessageConsumerMapping[] messageConsumerMappings = new MessageConsumerMapping[MESSAGE_CONSUMER_DEFAULT_CAPACITY];


    private final ManyToOneConcurrentArrayQueue<MessagingCommand> commandQueue = new ManyToOneConcurrentArrayQueue<MessagingCommand>(COMMAND_QUEUE_DEFAULT_CAPACITY);

    private AeronTransportContext aeronTransportContext;
    private AmqpProtonTransportContext amqpTransportContext;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    private final ExecutorService sendThreadExecutor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean sendThreadRunning = new AtomicBoolean(true);
    private final IdleStrategy sendThreadIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    private final ExecutorService receiveThreadExecutor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean receiveThreadRunning = new AtomicBoolean(true);
    private final IdleStrategy receiveThreadIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    private synchronized AeronTransportContext getAeronTransportContext()
    {
        if(aeronTransportContext == null)
        {
            aeronTransportContext = new AeronTransportContext();
        }
        return aeronTransportContext;
    }

    private synchronized  AmqpProtonTransportContext getAmqpTransportContext()
    {
        if(amqpTransportContext == null)
        {
            amqpTransportContext = new AmqpProtonTransportContext();
        }
        return amqpTransportContext;
    }
    /**
     * Intended for read only access to the transports
     * @return the atomic array of transports
     */
    protected AtomicArray<ReceivingTransport> getPollableReceivingTransports()
    {
        return pollableReceivingTransports;
    }

    public boolean enqueueCommand(MessagingCommand command)
    {
        return commandQueue.offer(command);
    }


    private int findNextProducerIndex()
    {
        int index = -1;
        for(int i = 0; i < messageProducerMappings.length && index == -1; i++)
        {
            if(messageProducerMappings[i] == null)
            {
                index = i;
            }
        }
        return index;
    }

    private int findNextConsumerIndex()
    {
        int index = -1;
        for(int i = 0; i < messageConsumerMappings.length && index == -1; i++)
        {
            if(messageConsumerMappings[i] == null)
            {
                index = i;
            }
        }
        return index;
    }

    private MessageProducerMapping createMessageProducer(MessageFlow messageFlow, int index)
    {
        final MessageProducerMapping producerMapping = new MessageProducerMapping(messageFlow, index);
        messageProducerMappings[index] = producerMapping;
        producerSendQueues.add(producerMapping.getSendQueue());
        long hash = producerMapping.getMessageFlow().getLogicalName().hashCode();
        AtomicArray<MessageProducerMapping> hashMatches = logicalNameToProducerMap.computeIfAbsent(hash, (ignore) -> new AtomicArray<MessageProducerMapping>());
        hashMatches.add(producerMapping);

        //Discovery callback may come from a different thread
        Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction = new Consumer<DiscoveryEvent<DiscoverableTransport>>()
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
                        DiscoverableTransport discoverableTransport = added.get(i);
                        TransportHandle transportHandle = discoverableTransport.getTransportHandle();
                        if(transportHandle != null)
                        {
                            //TODO(JAF): Refactor to avoid capture
                            final String targetTransportHandleId = transportHandle.getId();

                            SendingTransport match = producerMapping.getSendingTransports().findFirst(
                                    (sendingTransport) -> targetTransportHandleId.equals(sendingTransport.getTargetTransportHandleId())
                            );


                            if(match == null)
                            {
                                SendingTransport sendingTransport = createSendingTransportFromHandle(transportHandle);
                                MessagingCommand messagingCommand = new MessagingCommand();
                                messagingCommand.setMessageProducerIndex(producerMapping.getIndex());
                                messagingCommand.setType(MessagingCommand.TYPE_ADD_SENDING_TRANSPORT);
                                messagingCommand.setSendingTransport(sendingTransport);
                                enqueueCommand(messagingCommand);
                            }
                        }
                    }
                }

                if(removed != null)
                {
                    for(int i = 0; i < removed.size(); i++)
                    {
                        DiscoverableTransport discoverableTransport = removed.get(i);
                        TransportHandle transportHandle = discoverableTransport.getTransportHandle();

                        if(transportHandle != null)
                        {
                            //TODO(JAF): Refactor to avoid capture
                            final String targetTransportHandleId = transportHandle.getId();
                            if(targetTransportHandleId != null)
                            {
                                producerMapping.getSendingTransports().forEach((sendingTransport) ->
                                {
                                    if (targetTransportHandleId.equals(sendingTransport.getTargetTransportHandleId())) {
                                        MessagingCommand messagingCommand = new MessagingCommand();
                                        messagingCommand.setMessageProducerIndex(producerMapping.getIndex());
                                        messagingCommand.setType(MessagingCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT);
                                        messagingCommand.setSendingTransport(sendingTransport);
                                        enqueueCommand(messagingCommand);
                                    }
                                });
                            }
                        }
                    }
                }
            }
        };


        if(producerMapping.getMessageFlow().requiresDiscovery())
        {
            if (discoveryService != null)
            {
                discoveryService.addDiscoveryEventListener(messageFlow.getLogicalName(), discoveredTransportsAction);

                //Deliver any initial matching values
                AtomicArray<DiscoverableTransport> matchingValues = discoveryService.getValues(messageFlow.getLogicalName());
                if(matchingValues != null) {
                    final DiscoveryEvent<DiscoverableTransport> discoveryEvent = new DiscoveryEvent<>();
                    matchingValues.forEach((discoverableTransport) -> discoveryEvent.getAdded().add(discoverableTransport));
                    discoveredTransportsAction.accept(discoveryEvent);
                }

            }
            else
            {
                throw new UnsupportedOperationException("Cannot resolve sending transports since discovery service is null");
            }
        }
        else
        {
            //Create a single sending transport if possible when there is no discovery service
            SendingTransport sendingTransport = createSendingTransport(producerMapping.getMessageFlow());
            producerMapping.getSendingTransports().add(sendingTransport);
        }
        return producerMapping;
    }

    private void deleteMessageProducer(int index)
    {
        MessageProducerMapping producerMapping = messageProducerMappings[index];
        if(producerMapping != null)
        {
            if(discoveryService != null && producerMapping.getDiscoveredTransportsAction() != null)
            {
                discoveryService.removeDiscoveryEventListener(producerMapping.getMessageFlow().getLogicalName(), producerMapping.getDiscoveredTransportsAction());
            }

            long hash = producerMapping.getMessageFlow().getLogicalName().hashCode();
            AtomicArray<MessageProducerMapping> hashMatches = logicalNameToProducerMap.get(hash);
            hashMatches.remove(producerMapping);

            producerSendQueues.remove(producerMapping.getSendQueue());

            producerMapping.getSendingTransports().forEach((sendingTransport) -> sendingTransport.close());
            producerMapping.getSendingTransports().clear();

            messageProducerMappings[index] = null;
        }
    }

    private MessageConsumerMapping createMessageConsumer(MessageFlow messageFlow, Consumer<Message> messageHandler, int index)
    {
        MessageConsumerMapping consumerMapping = new MessageConsumerMapping(messageFlow);
        messageConsumerMappings[index] = consumerMapping;
        ReceivingTransport receivingTransport = createReceivingTransport(messageFlow, messageHandler);
        if(discoveryService != null)
        {
            DiscoverableTransport discoverableTransport = new DiscoverableTransport(messageFlow.getLogicalName(), receivingTransport.getHandle());
            receivingTransport.setDiscoverableTransport(discoverableTransport);
            discoveryService.registerValue(discoverableTransport.getKey(), discoverableTransport);
        }
        consumerMapping.getReceivingTransports().add(receivingTransport);
        if(receivingTransport.isPollable()) {
            pollableReceivingTransports.add(receivingTransport);
        }
        return consumerMapping;
    }

    private void deleteMessageConsumer(long id)
    {
        MessageConsumerMapping consumerMapping = null;
        //Scanning for id is fine during delete
        for(int i = 0; i < messageConsumerMappings.length && consumerMapping == null; i++)
        {
            if(messageConsumerMappings[i] != null && messageConsumerMappings[i].getId() == id)
            {
                consumerMapping = messageConsumerMappings[i];

                consumerMapping.getReceivingTransports().forEach(receivingTransport ->
                {
                    if(discoveryService != null && receivingTransport.getDiscoverableTransport() != null)
                    {
                        DiscoverableTransport discoverableTransport = receivingTransport.getDiscoverableTransport();
                        discoveryService.unregisterValue(discoverableTransport.getKey(), discoverableTransport);
                    }

                    if(receivingTransport.isPollable())
                    {
                        pollableReceivingTransports.remove(receivingTransport);
                    }
                    receivingTransport.close();
                });

                consumerMapping.getReceivingTransports().clear();

                messageConsumerMappings[i] = null;
            }
        }
    }

    private final Consumer<MessagingCommand> commandConsumer = new Consumer<MessagingCommand>() {
        @Override
        public void accept(MessagingCommand messagingCommand) {

            if(messagingCommand.getType() == MessagingCommand.TYPE_CREATE_PRODUCER)
            {
                int index = findNextProducerIndex();
                messagingCommand.setMessageProducerIndex(index);

                if(index != -1)
                {
                    MessageProducerMapping messageProducerMapping = createMessageProducer(messagingCommand.getMessageFlow(), index);
                    messagingCommand.setMessageProducerId(messageProducerMapping.getId());
                    messagingCommand.setFreeQueue(messageProducerMapping.getFreeQueue());
                    messagingCommand.setSendQueue(messageProducerMapping.getSendQueue());
                }
                else
                {
                    LOGGER.warn("Failed to create message producer because no index is available due to current size={}", messageProducerMappings.length);
                }
            }
            else if(messagingCommand.getType() == MessagingCommand.TYPE_DELETE_PRODUCER)
            {
                deleteMessageProducer(messagingCommand.getMessageProducerIndex());
            }
            else if(messagingCommand.getType() == MessagingCommand.TYPE_CREATE_CONSUMER)
            {
                int index = findNextConsumerIndex();
                MessageConsumerMapping messageConsumerMapping = createMessageConsumer(messagingCommand.getMessageFlow(), messagingCommand.getMessageHandler(), index);
                messagingCommand.setMessageConsumerId(messageConsumerMapping.getId());
            }
            else if(messagingCommand.getType() == MessagingCommand.TYPE_DELETE_CONSUMER)
            {
                deleteMessageConsumer(messagingCommand.getMessageConsumerId());
            }

            else if(messagingCommand.getType() == MessagingCommand.TYPE_ADD_SENDING_TRANSPORT && messagingCommand.getSendingTransport() != null)
            {
                MessageProducerMapping producerMapping = messageProducerMappings[messagingCommand.getMessageProducerIndex()];
                if(producerMapping != null)
                {
                    producerMapping.getSendingTransports().add(messagingCommand.getSendingTransport());
                }
            }
            else if(messagingCommand.getType() == MessagingCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT && messagingCommand.getSendingTransport() != null)
            {
                MessageProducerMapping producerMapping = messageProducerMappings[messagingCommand.getMessageProducerIndex()];
                if(producerMapping != null)
                {
                    producerMapping.getSendingTransports().remove(messagingCommand.getSendingTransport());
                }

                messagingCommand.getSendingTransport().close();
            }

            if(messagingCommand.getCommandCompletedAction() != null)
            {
                messagingCommand.getCommandCompletedAction().accept(messagingCommand);
            }
        }
    };

    public MessagingDriver()
    {
        executor.execute(() -> runCommandThread());
        sendThreadExecutor.execute(() -> runSendThread());
        receiveThreadExecutor.execute(() -> runReceiveThread());
    }

    protected int doWork()
    {
        return 0;
    }

    protected void runCommandThread()
    {
        try
        {
            while (running.get())
            {
                commandQueue.drain(commandConsumer);
                int workDone = doWork();
                idleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running command thread", ex);
        }
    }

    protected void runSendThread()
    {
        while (sendThreadRunning.get())
        {
            int workDone = producerSendQueues.doAction(0, (producerSendQueue) ->
            {
                int perProducerWorkDone = 0;
                int i = 0;
                Message queuedMessage = producerSendQueue.peek();
                while(queuedMessage != null && i < PER_PRODUCER_SEND_COUNT_LIMIT)
                {
                    MessageProducerMapping producerMapping = messageProducerMappings[queuedMessage.getMessageProducerIndex()];
                    if(producerMapping != null)
                    {
                        boolean result = producerMapping.getSendingTransports().doActionWithArgToBoolean(0,
                                (sendingTransport, message) -> sendingTransport.offer(message),
                                queuedMessage);

                        //TODO(JAF): Determine what to do with errors on some of the transports but not all (resend on those transports?)
                        perProducerWorkDone++;
                        Message dequeuedMessage = producerSendQueue.poll();
                        producerMapping.getFreeQueue().offer(dequeuedMessage);
                    }
                    i++;
                    queuedMessage = producerSendQueue.peek();
                }
                return perProducerWorkDone;
            });

            sendThreadIdleStrategy.idle(workDone);
        }
    }

    protected void runReceiveThread()
    {
        try
        {
            while (receiveThreadRunning.get())
            {

                int workDone = 0;
                if(aeronTransportContext != null)
                {
                    workDone += aeronTransportContext.doReceiveWork(getPollableReceivingTransports());
                }
                if(amqpTransportContext != null)
                {
                    workDone += amqpTransportContext.doReceiveWork(getPollableReceivingTransports());
                }
                receiveThreadIdleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running receive thread", ex);
        }
    }

    public SendingTransport createSendingTransportFromHandle(TransportHandle transportHandle)
    {
        SendingTransport sendingTransport = null;
        if(transportHandle.getType() == TransportHandle.Type.Aeron)
        {
            String channel = transportHandle.getPhysicalAddress();
            int streamId = 0;
            AeronUri aeronUri = AeronUri.parse(channel);
            if(aeronUri.get("streamId") != null)
            {
                streamId = Integer.parseInt(aeronUri.get("streamId"));
            }

            sendingTransport = new AeronSendingTransport(getAeronTransportContext(), channel, streamId, transportHandle.getId());
        }
        else if(transportHandle.getType() == TransportHandle.Type.AMQP)
        {
            String address = transportHandle.getPhysicalAddress();
            sendingTransport = new AmqpProtonSendingTransport(getAmqpTransportContext(), address, transportHandle.getId());
        }
        else
        {
            throw new UnsupportedOperationException("Not yet supporting this type of transport");
        }
        return sendingTransport;
    }

    public void setDiscoveryService(DiscoveryService<DiscoverableTransport> discoveryService)
    {
        this.discoveryService = discoveryService;
    }

    public void close()
    {
        running.set(false);
        executor.shutdown();
        sendThreadExecutor.shutdown();
        receiveThreadExecutor.shutdown();
        try
        {
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            sendThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            receiveThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        if(aeronTransportContext != null)
        {
            getAeronTransportContext().close();
            aeronTransportContext = null;
        }
        if(amqpTransportContext != null)
        {
            getAmqpTransportContext().close();
            amqpTransportContext = null;
        }
    }

    public SendingTransport createSendingTransport(MessageFlow messageFlow)
    {
        if (messageFlow instanceof Pipe)
        {
            Pipe pipe = (Pipe) messageFlow;
            String address = pipe.getLogicalName();
            if(address.startsWith("aeron"))
            {
                return new AeronSendingTransport(getAeronTransportContext(), pipe.getLogicalName(), pipe.getStreamId());
            }
            else if(address.startsWith("amqp"))
            {
                return new AmqpProtonSendingTransport(getAmqpTransportContext(), pipe.getLogicalName());
            }
            else
            {
                throw new UnsupportedOperationException("Not yet supporting this type of transport");
            }

        }
        else
        {
            throw new UnsupportedOperationException("Not yet supporting this type of message flow");
        }
    }

    public ReceivingTransport createReceivingTransport(MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        ReceivingTransport receivingTransport = null;

        if (messageFlow instanceof Pipe)
        {
            Pipe pipe = (Pipe) messageFlow;
            String address = messageFlow.getLogicalName();
            if(address.startsWith("aeron"))
            {
                receivingTransport = new AeronReceivingTransport(getAeronTransportContext(), pipe.getLogicalName(), pipe.getStreamId(), messageHandler);
            }
            else if(address.startsWith("amqp"))
            {
                receivingTransport = new AmqpProtonReceivingTransport(getAmqpTransportContext(), pipe.getLogicalName(), messageHandler);
            }
            else
            {
                throw new UnsupportedOperationException("Not yet supporting this type of transport");
            }
        }
        else if(messageFlow.requiresDiscovery())
        {
            //TODO(JAF): Determine how to indicate this is a discoverable AMQP receiving transport
            //receivingTransport = new AmqpProtonReceivingTransport(getAmqpTransportContext(), AmqpProtonTransportContext.DEFAULT_AMQP_SUBSCRIPTION_ADDRESS, messageHandler);

            String defaultSubscriptionChannel = AeronTransportContext.DEFAULT_AERON_SUBSCRIPTION_CHANNEL;
            AeronUri aeronUri = AeronUri.parse(defaultSubscriptionChannel);
            String channel = defaultSubscriptionChannel;
            int streamId = 0;
            if(aeronUri.get("streamId") != null)
            {
                streamId = Integer.parseInt(aeronUri.get("streamId"));
            }
            else
            {
                streamId = AeronTransportContext.globalStreamIdCtr.getAndIncrement();
                channel = channel + "|streamId=" + streamId;
            }

            receivingTransport = new AeronReceivingTransport(getAeronTransportContext(), channel, streamId, messageHandler);
        }
        else
        {
            throw new UnsupportedOperationException("Not yet supporting this type of message flow");
        }
        return receivingTransport;
    }
}
