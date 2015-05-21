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
package org.kaazing.messaging.common.transport;

import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.destination.MessageFlow;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.AtomicArray;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class BaseTransportContext implements TransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTransportContext.class);

    private static final int COMMAND_QUEUE_DEFAULT_CAPACITY = 1024;
    private DiscoveryService<DiscoverableTransport> discoveryService;
    //Single writer with lock free readers
    private final AtomicArray<ReceivingTransport> receivingTransports = new AtomicArray<ReceivingTransport>();
    //Single writer with lock free readers
    private final AtomicArray<SendingTransport> sendingTransports = new AtomicArray<SendingTransport>();

    private final ManyToOneConcurrentArrayQueue<TransportCommand> commandQueue = new ManyToOneConcurrentArrayQueue<TransportCommand>(COMMAND_QUEUE_DEFAULT_CAPACITY);

    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy(
            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

    /**
     * Intended for read only access to the transports
     * @return the atomic array of transports
     */
    protected AtomicArray<ReceivingTransport> getReceivingTransports()
    {
        return receivingTransports;
    }

    /**
     * Intended for read only access to the transports
     * @return the atomic array of transports
     */
    protected AtomicArray<SendingTransport> getSendingTransports()
    {
        return sendingTransports;
    }

    @Override
    public boolean enqueueCommand(TransportCommand command)
    {
        return commandQueue.offer(command);
    }

    private final Consumer<TransportCommand> transportCommandConsumer = new Consumer<TransportCommand>() {
        @Override
        public void accept(TransportCommand transportCommand) {

            if(transportCommand.getType() == TransportCommand.TYPE_ADD_RECEIVING_TRANSPORT)
            {
                if (transportCommand.getReceivingTransport() != null && transportCommand.getReceivingTransport().isPollable())
                {
                    receivingTransports.add(transportCommand.getReceivingTransport());
                    if(discoveryService != null && transportCommand.getReceivingTransport().getDiscoverableTransport()!= null)
                    {
                        DiscoverableTransport discoverableTransport = transportCommand.getReceivingTransport().getDiscoverableTransport();
                        discoveryService.registerValue(discoverableTransport.getKey(), discoverableTransport);
                    }
                }
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT)
            {
                if (transportCommand.getReceivingTransport() != null && transportCommand.getReceivingTransport().isPollable())
                {
                    receivingTransports.remove(transportCommand.getReceivingTransport());
                    //Don't do the close here as it will be done by the message consumer in the current implementation

                    if(discoveryService != null && transportCommand.getReceivingTransport().getDiscoverableTransport() != null)
                    {
                        DiscoverableTransport discoverableTransport = transportCommand.getReceivingTransport().getDiscoverableTransport();
                        discoveryService.unregisterValue(discoverableTransport.getKey(), discoverableTransport);
                    }
                }
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_ADD_DISCOVERY_LISTENER)
            {
                if(discoveryService != null && transportCommand.getDiscoveryKey() != null && transportCommand.getDiscoveredTransportsAction() != null)
                {
                    discoveryService.addDiscoveryEventListener(transportCommand.getDiscoveryKey(), transportCommand.getDiscoveredTransportsAction());

                    //Deliver any initial matching values
                    AtomicArray<DiscoverableTransport> matchingValues = discoveryService.getValues(transportCommand.getDiscoveryKey());
                    if(matchingValues != null) {
                        final DiscoveryEvent<DiscoverableTransport> discoveryEvent = new DiscoveryEvent<>();
                        matchingValues.forEach((discoverableTransport) -> discoveryEvent.getAdded().add(discoverableTransport));
                        transportCommand.getDiscoveredTransportsAction().accept(discoveryEvent);
                    }
                }
            }
            else if(transportCommand.getType() == TransportCommand.TYPE_REMOVE_DISCOVERY_LISTENER)
            {
                if(discoveryService != null && transportCommand.getDiscoveryKey() != null && transportCommand.getDiscoveredTransportsAction() != null)
                {
                    discoveryService.removeDiscoveryEventListener(transportCommand.getDiscoveryKey(), transportCommand.getDiscoveredTransportsAction());
                }
            }


            if(transportCommand.getCommandCompletedAction() != null)
            {
                transportCommand.getCommandCompletedAction().accept(transportCommand);
            }
        }
    };

    public BaseTransportContext()
    {
        executor.execute(() -> runMainLoop());
    }

    protected int doWork()
    {
        return 0;
    }

    protected void runMainLoop()
    {
        try
        {
            while (running.get())
            {
                commandQueue.drain(transportCommandConsumer);
                int workDone = doWork();
                idleStrategy.idle(workDone);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Override
    public List<SendingTransport> createSendingTransports(MessageFlow messageFlow)
    {
        List<SendingTransport> sendingTransports = null;
        if(messageFlow.requiresDiscovery())
        {
            if (discoveryService != null)
            {
                LOGGER.debug("Initial sending transports (if any) will be delivered via a discovery event for this MessageFlow {}", messageFlow.getLogicalName());
            }
            else
            {
                throw new UnsupportedOperationException("Cannot resolve sending transports since discovery service is null");
            }
        }
        else
        {
            sendingTransports = new ArrayList<SendingTransport>();
            //Create a single sending transport if possible when there is no discovery service
            SendingTransport sendingTransport = TransportFactory.createSendingTransport(this, messageFlow);
            sendingTransports.add(sendingTransport);
        }
        return sendingTransports;
    }

    @Override
    public SendingTransport createSendingTransportFromHandle(TransportHandle transportHandle)
    {
        return TransportFactory.createSendingTransportFromHandle(this, transportHandle);

    }

    @Override
    public List<ReceivingTransport> createReceivingTransports(MessageFlow messageFlow, Consumer<Message> messageHandler)
    {
        List<ReceivingTransport> receivingTransports = new ArrayList<ReceivingTransport>();
        ReceivingTransport receivingTransport = TransportFactory.createReceivingTransport(this, messageFlow, messageHandler);
        if(discoveryService != null)
        {
            DiscoverableTransport discoverableTransport = new DiscoverableTransport(messageFlow.getLogicalName(), receivingTransport.getHandle());
            receivingTransport.setDiscoverableTransport(discoverableTransport);
        }
        receivingTransports.add(receivingTransport);
        return receivingTransports;
    }

    @Override
    public void setDiscoveryService(DiscoveryService<DiscoverableTransport> discoveryService)
    {
        this.discoveryService = discoveryService;
    }

    @Override
    public void close()
    {
        running.set(false);
        executor.shutdown();
        try
        {
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }


}
