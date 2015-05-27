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
package org.kaazing.messaging.common.transport.amqp;

import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.impl.MessengerImpl;
import org.kaazing.messaging.common.transport.ReceivingTransport;
import org.kaazing.messaging.common.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.AtomicArray;


import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AmqpProtonTransportContext implements TransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpProtonTransportContext.class);

    //TODO(JAF): Change this to be an interface scan instead of localhost
    public static final String DEFAULT_AMQP_SUBSCRIPTION_ADDRESS = "amqp://~127.0.0.1:5672";
    private final Messenger messenger;

    private final AtomicInteger numSubscribers = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Consumer<Message>> subscriptions = new ConcurrentHashMap<String, Consumer<Message>>();

    public AmqpProtonTransportContext()
    {
        super();
        messenger = new MessengerImpl();
        try {
            messenger.start();
        } catch (IOException e) {
            LOGGER.error("Error starting proton messenger", e);
        }
    }

    protected Messenger getMessenger()
    {
        return messenger;
    }

    protected void addSubscription(String address, Consumer<Message> subscription)
    {

        String key = address.replace("~","");
        Consumer<Message> existing = subscriptions.putIfAbsent(key, subscription);
        if(existing != null)
        {
            //TODO(JAF): Do we need to support a list of consumers at a given address instead of replacing
            throw new UnsupportedOperationException("Not supported to have more than one receiving transport on this address");
        }
        numSubscribers.incrementAndGet();
    }

    protected void removeSubscription(String address, Consumer<Message> subscription)
    {
        subscriptions.remove(address, subscription);
    }

    public int doReceiveWork(AtomicArray<ReceivingTransport> receivingTransports)
    {
        int workDone = 0;
        if(numSubscribers.get() > 0)
        {
            //TODO(JAF): Fix the concurrent modification exception when removing the last subscriber
            messenger.recv();
            while (messenger.incoming() > 0)
            {
                Message message = messenger.get();
                workDone++;

                //TODO(JAF): Do we need to support a list of consumers at a given address instead of replacing
                //TODO(JAF): Need a way to identify a message is for a specific subscriber other than current address matching
                Consumer<Message> receivingTransport = subscriptions.get(message.getAddress());
                if (receivingTransport != null)
                {
                    receivingTransport.accept(message);
                }
            }
        }
        return workDone;
    }

    @Override
    public void close()
    {
        messenger.stop();
    }
}
