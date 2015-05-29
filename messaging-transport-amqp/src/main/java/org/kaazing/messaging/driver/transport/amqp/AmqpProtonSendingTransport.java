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
package org.kaazing.messaging.driver.transport.amqp;

import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.kaazing.messaging.driver.transport.TransportContext;

public class AmqpProtonSendingTransport implements SendingTransport
{
    private final String address;
    private String targetTransportHandleId;
    private final AmqpProtonTransportContext amqpTransportContext;
    private final ThreadLocal<org.apache.qpid.proton.message.Message> tlAmqpMessage = new ThreadLocal<org.apache.qpid.proton.message.Message>().withInitial(() -> new MessageImpl());

    public AmqpProtonSendingTransport(AmqpProtonTransportContext amqpTransportContext, String address)
    {
        this.amqpTransportContext = amqpTransportContext;
        this.address = address;
    }

    public AmqpProtonSendingTransport(AmqpProtonTransportContext amqpTransportContext, String address, String targetTransportHandleId)
    {
        this.amqpTransportContext = amqpTransportContext;
        this.address = address;
        this.targetTransportHandleId = targetTransportHandleId;
    }

    @Override
    public TransportContext getTransportContext()
    {
        return amqpTransportContext;
    }

    @Override
    public void submit(DriverMessage driverMessage)
    {
        org.apache.qpid.proton.message.Message amqpMessage = tlAmqpMessage.get();
        amqpMessage.setAddress(address);

        driverMessage.getBuffer();
        byte[] bytesToSend = new byte[driverMessage.getBufferLength()];
        driverMessage.getBuffer().getBytes(driverMessage.getBufferOffset(), bytesToSend);
        Binary binary = new Binary(bytesToSend);
        amqpMessage.setBody(new Data(binary));

        amqpTransportContext.getMessenger().put(amqpMessage);

        //TODO(JAF) See if this should be moved elsewhere to do batching
        amqpTransportContext.getMessenger().send();
    }

    @Override
    public long offer(DriverMessage driverMessage)
    {
        submit(driverMessage);
        //TODO(JAF): Support offer method with AMQP
        //throw new UnsupportedOperationException("non-blocking submit method is not supported with this transport");
        return 0;
    }

    @Override
    public void close()
    {
        //Nothing specific to do
    }

    @Override
    public String getTargetTransportHandleId()
    {
        return targetTransportHandleId;
    }
}
