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
package org.kaazing.messaging.driver.transport.netty.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.LengthFieldPrepender;
import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.transport.SendingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;

public class NettySendingTransport implements SendingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NettySendingTransport.class);

    private final String address;
    private final InetSocketAddress inetSocketAddress;
    private String targetTransportHandleId;
    private final NettyTransportContext nettyTransportContext;
    private final ThreadLocal<ByteBuf> tlNettyMessage;
    private final Channel sendingChannel;

    public NettySendingTransport(NettyTransportContext nettyTransportContext, String address)
    {
        this(nettyTransportContext, address, null);
    }

    public NettySendingTransport(NettyTransportContext nettyTransportContext, String address, String targetTransportHandleId)
    {
        this.nettyTransportContext = nettyTransportContext;
        this.address = address;
        this.targetTransportHandleId = targetTransportHandleId;

        final URI uri;
        try {
            uri = new URI(address);
            final int uriPort = uri.getPort();
            if (uriPort < 0)
            {
                throw new IllegalArgumentException("Port must be specified");
            }
            final InetAddress hostAddress = InetAddress.getByName(uri.getHost());
            this.inetSocketAddress = new InetSocketAddress(hostAddress, uriPort);
            sendingChannel = nettyTransportContext.getBootstrap().connect(hostAddress, uriPort).sync().channel();
            sendingChannel.pipeline().addLast(new LengthFieldPrepender(4));

        } catch (URISyntaxException e) {
            LOGGER.error("Error parsing address", e);
            throw new IllegalArgumentException("Failed to parse address: " + address);
        } catch (UnknownHostException e) {
            LOGGER.error("Error resolving host", e);
            throw new IllegalArgumentException("Failed to resolve host: " + address);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted", e);
            throw new IllegalArgumentException("Interrupted while binding to address: " + address);
        }

        tlNettyMessage = new ThreadLocal<ByteBuf>().withInitial(() -> Unpooled.buffer());

    }

    @Override
    public TransportContext getTransportContext()
    {
        return nettyTransportContext;
    }

    @Override
    public void submit(DriverMessage driverMessage)
    {
        try {
            writeAndFlush(driverMessage).sync().await();
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while sending message");
        }
    }

    @Override
    public long offer(DriverMessage driverMessage)
    {
        writeAndFlush(driverMessage);
        return 0;
    }

    private ChannelFuture writeAndFlush(DriverMessage driverMessage)
    {
        //TODO(JAF): Figure out why multiple sends are getting condensed down to a single send
        ByteBuf nettyMessage = tlNettyMessage.get();
        nettyMessage.retain();

        //TODO(JAF): Avoid making a new byte array with each send
        byte[] bytesToSend = new byte[driverMessage.getBufferLength()];
        driverMessage.getBuffer().getBytes(driverMessage.getBufferOffset(), bytesToSend);
        nettyMessage.setBytes(0, bytesToSend);
        nettyMessage.writerIndex(bytesToSend.length);

        return sendingChannel.writeAndFlush(nettyMessage);
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
