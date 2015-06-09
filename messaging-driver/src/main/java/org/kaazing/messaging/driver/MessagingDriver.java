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

import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.common.collections.AtomicArrayWithArg;
import org.kaazing.messaging.driver.command.ClientCommand;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.discovery.DiscoveryEvent;
import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.transport.*;
import org.kaazing.messaging.driver.mapping.MessageConsumerMapping;
import org.kaazing.messaging.driver.mapping.MessageProducerMapping;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.kaazing.messaging.driver.transport.TransportFactory;

import org.kaazing.messaging.driver.command.DriverCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.driver.uri.AeronUri;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.*;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class MessagingDriver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingDriver.class);

    private static final int COMMAND_QUEUE_DEFAULT_CAPACITY = 1024;
    //private static final int SEND_QUEUE_DEFAULT_CAPACITY = 4096;
    private static final int MESSAGE_PRODUCER_DEFAULT_CAPACITY = 512;
    private static final int MESSAGE_CONSUMER_DEFAULT_CAPACITY = 512;

    private final static int PER_PRODUCER_SEND_COUNT_LIMIT = 10;

    //TODO(JAF): Change this to be an interface scan instead of localhost
    public static final String DEFAULT_AERON_SUBSCRIPTION_CHANNEL = "aeron:udp?remote=127.0.0.1:40123";
    public static final AtomicInteger GLOBAL_STREAM_ID_CTR = new AtomicInteger(10);

    private final int numThreads;
    private DiscoveryService<DiscoverableTransport> discoveryService;

    private final ConcurrentHashMap<String, TransportFactory> cachedTransportFactories = new ConcurrentHashMap<String, TransportFactory>();

    private final AtomicArrayWithArg<TransportContext, AtomicArray<ReceivingTransport>> activeTransportContexts = new AtomicArrayWithArg<>();
    private final ConcurrentHashMap<String, TransportContext> transportContextsMap = new ConcurrentHashMap<>();
    //Single writer with lock free readers
    private final AtomicArray<ReceivingTransport> pollableReceivingTransports = new AtomicArray<ReceivingTransport>();

    private final MessageProducerMapping[] messageProducerMappings = new MessageProducerMapping[MESSAGE_PRODUCER_DEFAULT_CAPACITY];
    private Long2ObjectHashMap<AtomicArray<MessageProducerMapping>> logicalNameToProducerMap = new Long2ObjectHashMap<>();
    private final AtomicArray<OneToOneConcurrentArrayQueue<DriverMessage>> producerSendQueues = new AtomicArray<>();

    private final MessageConsumerMapping[] messageConsumerMappings = new MessageConsumerMapping[MESSAGE_CONSUMER_DEFAULT_CAPACITY];


    private final ManyToOneConcurrentArrayQueue<ClientCommand> clientCommandQueue = new ManyToOneConcurrentArrayQueue<>(COMMAND_QUEUE_DEFAULT_CAPACITY);
    private final ManyToOneConcurrentArrayQueue<DriverCommand> driverCommandQueue = new ManyToOneConcurrentArrayQueue<>(COMMAND_QUEUE_DEFAULT_CAPACITY);

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

    private final ExecutorService sendReceiveThreadExecutor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean sendReceiveThreadRunning = new AtomicBoolean(true);
    private final IdleStrategy sendReceiveThreadIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    private final ExecutorService commandSendReceiveThreadExecutor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean commandSendReceiveThreadRunning = new AtomicBoolean(true);
    private final IdleStrategy commandSendReceiveThreadIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));


    private final IdleStrategy deleteIdleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    /**
     * Intended for read only access to the transports
     * @return the atomic array of transports
     */
    protected AtomicArray<ReceivingTransport> getPollableReceivingTransports()
    {
        return pollableReceivingTransports;
    }



    private static class MessagingSingletonLoader
    {
        private static final MessagingDriver INSTANCE = new MessagingDriver(true);
    }

    //Only called by MessagingSingletonLoader
    private MessagingDriver(boolean isSingleton) {
        this();
        if (MessagingSingletonLoader.INSTANCE != null) {
            throw new IllegalStateException("Already instantiated");
        }
    }

    public static MessagingDriver getInstance() {
        return MessagingSingletonLoader.INSTANCE;
    }


    public MessagingDriver()
    {
        this(3);
    }
    public MessagingDriver(int numEmbeddedThreads)
    {
        LOGGER.debug("Loading TransportFactory implementations...");
        ServiceLoader<TransportFactory> transportFactoryServiceLoader = ServiceLoader.load(TransportFactory.class);
        Iterator<TransportFactory> iterator = transportFactoryServiceLoader.iterator();
        while(iterator.hasNext())
        {
            TransportFactory transportFactory = iterator.next();
            LOGGER.debug("Found TransportFactory with scheme={}", transportFactory.getScheme());
            cachedTransportFactories.put(transportFactory.getScheme(), transportFactory);
        }

        this.numThreads = numEmbeddedThreads;
        if(numEmbeddedThreads >= 3) {
            executor.execute(() -> runCommandThread());
            sendThreadExecutor.execute(() -> runSendThread());
            receiveThreadExecutor.execute(() -> runReceiveThread());
        }
        else if(numEmbeddedThreads == 2)
        {
            executor.execute(() -> runCommandThread());
            sendReceiveThreadExecutor.execute(() -> runSendReceiveThread());
        }
        else if(numEmbeddedThreads == 1)
        {
            commandSendReceiveThreadExecutor.execute(() -> runCommandSendReceiveThread());
        }
        else if(numEmbeddedThreads == 0)
        {
            LOGGER.debug("Running 0 embedded threads; media driver work must be explicitly executed");
        }
        else
        {
            throw new IllegalArgumentException("Invalid number of threads to use: " + numEmbeddedThreads);
        }
    }

    public boolean enqueueClientCommand(ClientCommand command)
    {
        return clientCommandQueue.offer(command);
    }

    public boolean enqueueDriverCommand(DriverCommand command)
    {
        return driverCommandQueue.offer(command);
    }

    public TransportFactory registerTransportFactory(String scheme, TransportFactory transportFactory)
    {
        return cachedTransportFactories.put(scheme, transportFactory);
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
        long hash = producerMapping.getMessageFlow().getName().hashCode();
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
                                DriverCommand driverCommand = new DriverCommand(DriverCommand.TYPE_ADD_SENDING_TRANSPORT);
                                driverCommand.setMessageProducerIndex(producerMapping.getIndex());
                                driverCommand.setSendingTransport(sendingTransport);
                                enqueueDriverCommand(driverCommand);
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
                                        DriverCommand driverCommand = new DriverCommand(DriverCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT);
                                        driverCommand.setMessageProducerIndex(producerMapping.getIndex());
                                        driverCommand.setSendingTransport(sendingTransport);
                                        enqueueDriverCommand(driverCommand);
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
                discoveryService.addDiscoveryEventListener(messageFlow.getName(), discoveredTransportsAction);

                //Deliver any initial matching values
                AtomicArray<DiscoverableTransport> matchingValues = discoveryService.getValues(messageFlow.getName());
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
                discoveryService.removeDiscoveryEventListener(producerMapping.getMessageFlow().getName(), producerMapping.getDiscoveredTransportsAction());
            }

            long hash = producerMapping.getMessageFlow().getName().hashCode();
            AtomicArray<MessageProducerMapping> hashMatches = logicalNameToProducerMap.get(hash);
            hashMatches.remove(producerMapping);

            //TODO(JAF): Implement a better way to delay closing until the send queue has a chance to be drained
//            int maxWaitSeconds = 1;
//            long start = System.nanoTime();
//            int queueSize = producerMapping.getSendQueue().size();
//            while(queueSize > 0)
//            {
//                deleteIdleStrategy.idle(0);
//                long now = System.nanoTime();
//                if(TimeUnit.NANOSECONDS.toSeconds(now - start) > maxWaitSeconds)
//                {
//                    break;
//                }
//                queueSize = producerMapping.getSendQueue().size();
//            }


            producerSendQueues.remove(producerMapping.getSendQueue());

            producerMapping.getSendingTransports().forEach((sendingTransport) -> sendingTransport.close());
            producerMapping.getSendingTransports().clear();

            messageProducerMappings[index] = null;
        }
    }

    private MessageConsumerMapping createMessageConsumer(MessageFlow messageFlow, Consumer<DriverMessage> messageHandler, int index)
    {
        MessageConsumerMapping consumerMapping = new MessageConsumerMapping(messageFlow);
        messageConsumerMappings[index] = consumerMapping;
        ReceivingTransport receivingTransport = createReceivingTransport(messageFlow, messageHandler);
        if(discoveryService != null)
        {
            DiscoverableTransport discoverableTransport = new DiscoverableTransport(messageFlow.getName(), receivingTransport.getHandle());
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

    private final Consumer<ClientCommand> clientCommandConsumer = new Consumer<ClientCommand>() {
        @Override
        public void accept(ClientCommand clientCommand) {

            if(clientCommand.getType() == ClientCommand.TYPE_CREATE_PRODUCER)
            {
                int index = findNextProducerIndex();
                clientCommand.setMessageProducerIndex(index);

                if(index != -1)
                {
                    MessageProducerMapping messageProducerMapping = createMessageProducer(clientCommand.getMessageFlow(), index);
                    clientCommand.setMessageProducerId(messageProducerMapping.getId());
                    clientCommand.setFreeQueue(messageProducerMapping.getFreeQueue());
                    clientCommand.setSendQueue(messageProducerMapping.getSendQueue());
                }
                else
                {
                    LOGGER.warn("Failed to create message producer because no index is available due to current size={}", messageProducerMappings.length);
                }
            }
            else if(clientCommand.getType() == ClientCommand.TYPE_DELETE_PRODUCER)
            {
                deleteMessageProducer(clientCommand.getMessageProducerIndex());
            }
            else if(clientCommand.getType() == ClientCommand.TYPE_CREATE_CONSUMER)
            {
                int index = findNextConsumerIndex();
                MessageConsumerMapping messageConsumerMapping = createMessageConsumer(clientCommand.getMessageFlow(), clientCommand.getMessageHandler(), index);
                clientCommand.setMessageConsumerId(messageConsumerMapping.getId());
            }
            else if(clientCommand.getType() == ClientCommand.TYPE_DELETE_CONSUMER)
            {
                deleteMessageConsumer(clientCommand.getMessageConsumerId());
            }

            if(clientCommand.getCommandCompletedAction() != null)
            {
                clientCommand.getCommandCompletedAction().accept(clientCommand);
            }
        }
    };

    private final Consumer<DriverCommand> driverCommandConsumer = new Consumer<DriverCommand>() {
        @Override
        public void accept(DriverCommand driverCommand) {

            if(driverCommand.getType() == DriverCommand.TYPE_ADD_SENDING_TRANSPORT && driverCommand.getSendingTransport() != null)
            {
                MessageProducerMapping producerMapping = messageProducerMappings[driverCommand.getMessageProducerIndex()];
                if(producerMapping != null)
                {
                    producerMapping.getSendingTransports().add(driverCommand.getSendingTransport());
                }
            }
            else if(driverCommand.getType() == DriverCommand.TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT && driverCommand.getSendingTransport() != null)
            {
                MessageProducerMapping producerMapping = messageProducerMappings[driverCommand.getMessageProducerIndex()];
                if(producerMapping != null)
                {
                    producerMapping.getSendingTransports().remove(driverCommand.getSendingTransport());
                }

                driverCommand.getSendingTransport().close();
            }

            if(driverCommand.getCommandCompletedAction() != null)
            {
                driverCommand.getCommandCompletedAction().accept(driverCommand);
            }
        }
    };


    protected int doWork()
    {
        return 0;
    }


    public int doCommandWork()
    {
        int workDone = 0;
        workDone += clientCommandQueue.drain(clientCommandConsumer);
        workDone += driverCommandQueue.drain(driverCommandConsumer);
        workDone += doWork();
        return workDone;
    }

    protected void runCommandThread()
    {
        try
        {
            while (running.get())
            {
                int workDone = doCommandWork();
                idleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running command thread", ex);
        }
    }

    public int doSendWork()
    {
        int workDone = producerSendQueues.doAction(0, (producerSendQueue) ->
        {
            int perProducerWorkDone = 0;
            int i = 0;
            DriverMessage queuedDriverMessage = producerSendQueue.peek();
            while(queuedDriverMessage != null && i < PER_PRODUCER_SEND_COUNT_LIMIT)
            {
                MessageProducerMapping producerMapping = messageProducerMappings[queuedDriverMessage.getMessageProducerIndex()];
                if(producerMapping != null)
                {
                    boolean result = producerMapping.getSendingTransports().doActionWithArgToBoolean(0,
                            (sendingTransport, message) -> sendingTransport.offer(message),
                            queuedDriverMessage);

                    //TODO(JAF): Determine what to do with errors on some of the transports but not all (resend on those transports?)
                    perProducerWorkDone++;
                    DriverMessage dequeuedDriverMessage = producerSendQueue.poll();
                    producerMapping.getFreeQueue().offer(dequeuedDriverMessage);
                }
                i++;
                queuedDriverMessage = producerSendQueue.peek();
            }
            return perProducerWorkDone;
        });

        return workDone;
    }

    protected void runSendThread()
    {
        while (sendThreadRunning.get())
        {
            int workDone = doSendWork();
            sendThreadIdleStrategy.idle(workDone);
        }
    }

    public int doReceiveWork()
    {
        int workDone = activeTransportContexts.doActionWithArg(0,
                (transportContext, receivingTransports) -> transportContext.doReceiveWork(receivingTransports),
                getPollableReceivingTransports());
        return workDone;
    }

    protected void runReceiveThread()
    {
        try
        {
            while (receiveThreadRunning.get())
            {

                int workDone = doReceiveWork();
                receiveThreadIdleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running receive thread", ex);
        }
    }

    protected void runSendReceiveThread()
    {
        try
        {
            while (sendReceiveThreadRunning.get())
            {
                int workDone = doReceiveWork();
                workDone += doSendWork();
                sendReceiveThreadIdleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running send receive thread", ex);
        }
    }

    protected void runCommandSendReceiveThread()
    {
        try
        {
            while (commandSendReceiveThreadRunning.get())
            {
                int workDone = doReceiveWork();
                workDone += doSendWork();
                workDone += doCommandWork();
                commandSendReceiveThreadIdleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            LOGGER.error("Error running command send receive thread", ex);
        }
    }

    public SendingTransport createSendingTransportFromHandle(TransportHandle transportHandle)
    {
        SendingTransport sendingTransport = null;
        String scheme = transportHandle.getScheme();
        String address = transportHandle.getPhysicalAddress();

        if(scheme != null)
        {
            TransportContext transportContext = transportContextsMap.get(scheme);
            TransportFactory transportFactory = cachedTransportFactories.get(scheme);
            if(transportContext != null)
            {
                if(transportFactory != null)
                {
                    sendingTransport = transportFactory.createSendingTransport(transportContext, address, transportHandle.getId());
                }
                else
                {
                    throw new IllegalStateException("Cannot have a valid transportContext with no transportFactory for scheme " + scheme);
                }
            }
            else
            {
                if(transportFactory != null)
                {
                    transportContext = transportFactory.createTransportContext();
                    TransportContext existingContext = transportContextsMap.putIfAbsent(scheme, transportContext);
                    if(existingContext != null)
                    {
                        LOGGER.warn("existing transport context should have been null but was {} for scheme {}", existingContext, scheme);
                        transportContext.close();
                        transportContext = existingContext;
                    }
                    else
                    {
                        activeTransportContexts.add(transportContext);
                    }

                    sendingTransport = transportFactory.createSendingTransport(transportContext, address, transportHandle.getId());
                }
                else
                {
                    LOGGER.warn("Cannot create sending transport for scheme {} due to no matching transport factory", scheme);
                }
            }
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
        sendThreadRunning.set(false);
        receiveThreadRunning.set(false);

        if(numThreads >= 3)
        {
            executor.shutdown();
            sendThreadExecutor.shutdown();
            receiveThreadExecutor.shutdown();
        }
        else if(numThreads == 2)
        {
            executor.shutdown();
            sendReceiveThreadExecutor.shutdown();
        }
        else if(numThreads == 1)
        {
            commandSendReceiveThreadExecutor.shutdown();
        }

        try
        {
            if(numThreads >= 3)
            {
                executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                sendThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                receiveThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }
            else if(numThreads == 2)
            {
                executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                sendReceiveThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }
            else if(numThreads == 1)
            {
                commandSendReceiveThreadExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }
        }
        catch (InterruptedException e)
        {
            LOGGER.debug("Interrupted while awaiting termination", e);
        }

        transportContextsMap.clear();

        activeTransportContexts.forEach(transportContext -> {
            transportContext.close();
        });
        activeTransportContexts.clear();
    }

    public SendingTransport createSendingTransport(MessageFlow messageFlow)
    {
        SendingTransport sendingTransport = null;
        if (!messageFlow.requiresDiscovery())
        {

            String address = messageFlow.getName();

            //TODO(JAF): Replace with proper scheme parsing
            String scheme = address.split(":")[0];

            if(scheme != null)
            {
                TransportContext transportContext = transportContextsMap.get(scheme);
                TransportFactory transportFactory = cachedTransportFactories.get(scheme);
                if(transportContext != null)
                {
                    if(transportFactory != null)
                    {
                        sendingTransport = transportFactory.createSendingTransport(transportContext, address);
                    }
                    else
                    {
                        throw new IllegalStateException("Cannot have a valid transportContext with no transportFactory for scheme " + scheme);
                    }
                }
                else
                {
                    if(transportFactory != null)
                    {
                        transportContext = transportFactory.createTransportContext();
                        TransportContext existingContext = transportContextsMap.putIfAbsent(scheme, transportContext);
                        if(existingContext != null)
                        {
                            LOGGER.warn("existing transport context should have been null but was {} for scheme {}", existingContext, scheme);
                            transportContext.close();
                            transportContext = existingContext;
                        }
                        else
                        {
                            activeTransportContexts.add(transportContext);
                        }

                        sendingTransport = transportFactory.createSendingTransport(transportContext, address);
                    }
                    else
                    {
                        LOGGER.warn("Cannot create sending transport for scheme {} due to no matching transport factory", scheme);
                    }
                }
            }

        }
        else
        {
            throw new UnsupportedOperationException("Not yet supporting this type of message flow");
        }
        return sendingTransport;
    }

    public ReceivingTransport createReceivingTransport(MessageFlow messageFlow, Consumer<DriverMessage> messageHandler)
    {
        ReceivingTransport receivingTransport = null;
        String scheme = null;
        String address = null;
        if (!messageFlow.requiresDiscovery())
        {
            address = messageFlow.getName();
            scheme = address.split(":")[0];
        }
        else
        {
            //TODO(JAF): Determine how to indicate this is a discoverable AMQP receiving transport
            //receivingTransport = new AmqpProtonReceivingTransport(getAmqpTransportContext(), AmqpProtonTransportContext.DEFAULT_AMQP_SUBSCRIPTION_ADDRESS, messageHandler);
            //scheme = "amqp";

            scheme = "aeron";
            //TODO(JAF): This should be a scan instead of defaulting to localhost
            String defaultSubscriptionChannel = DEFAULT_AERON_SUBSCRIPTION_CHANNEL;
            AeronUri aeronUri = AeronUri.parse(defaultSubscriptionChannel);
            address = defaultSubscriptionChannel;

            int streamId = 0;
            if(aeronUri.get("streamId") != null)
            {
                streamId = Integer.parseInt(aeronUri.get("streamId"));
            }
            else
            {
                streamId = GLOBAL_STREAM_ID_CTR.getAndIncrement();
                address = address + "|streamId=" + streamId;
            }
        }

        if(scheme != null)
        {
            TransportContext transportContext = transportContextsMap.get(scheme);
            TransportFactory transportFactory = cachedTransportFactories.get(scheme);
            if(transportContext != null)
            {
                if(transportFactory != null)
                {
                    receivingTransport = transportFactory.createReceivingTransport(transportContext, address, messageHandler);
                }
                else
                {
                    throw new IllegalStateException("Cannot have a valid transportContext with no transportFactory for scheme " + scheme);
                }
            }
            else
            {
                if(transportFactory != null)
                {
                    transportContext = transportFactory.createTransportContext();
                    TransportContext existingContext = transportContextsMap.putIfAbsent(scheme, transportContext);
                    if(existingContext != null)
                    {
                        LOGGER.warn("existing transport context should have been null but was {} for scheme {}", existingContext, scheme);
                        transportContext.close();
                        transportContext = existingContext;
                    }
                    else
                    {
                        activeTransportContexts.add(transportContext);
                    }

                    receivingTransport = transportFactory.createReceivingTransport(transportContext, address, messageHandler);
                }
                else
                {
                    LOGGER.warn("Cannot create receiving transport for scheme {} due to no matching transport factory", scheme);
                }
            }
        }
        return receivingTransport;
    }
}
