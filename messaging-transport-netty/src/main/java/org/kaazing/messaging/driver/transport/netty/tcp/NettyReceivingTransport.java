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
import io.netty.channel.*;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import org.kaazing.messaging.common.transport.TransportHandle;
import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.driver.message.DriverMessage;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.UUID;
import java.util.function.Consumer;

public class NettyReceivingTransport implements ReceivingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyReceivingTransport.class);

    private final int defaultBufferSize = 4096;
    private final String address;
    private final NettyTransportContext nettyTransportContext;
    private final Consumer<DriverMessage> messageHandler;
    private final TransportHandle handle;
    private DiscoverableTransport discoverableTransport;
    private final Channel receivingChannel;
    private final InetSocketAddress inetSocketAddress;
    private final ThreadLocal<DriverMessage> tlMessage = new ThreadLocal<DriverMessage>().withInitial(() -> new DriverMessage(defaultBufferSize));
    private final boolean inAddrAny;

    public NettyReceivingTransport(NettyTransportContext nettyTransportContext, String address, Consumer<DriverMessage> messageHandler)
    {
        this.nettyTransportContext = nettyTransportContext;
        this.address = address;
        final URI uri;
        final int uriPort;
        try
        {
            uri = new URI(address);
            uriPort = uri.getPort();
            if (uriPort < 0)
            {
                throw new IllegalArgumentException("Port must be specified");
            }
            final InetAddress hostAddress = InetAddress.getByName(uri.getHost());
            this.inetSocketAddress = new InetSocketAddress(hostAddress, uriPort);
            receivingChannel = nettyTransportContext.getServerBootstrap().bind(hostAddress, uriPort).sync().channel();
            //receivingChannel.pipeline().addLast(nettyChannelHandlerAdapter);

        }
        catch (URISyntaxException e)
        {
            LOGGER.error("Error parsing address", e);
            throw new IllegalArgumentException("Failed to parse address: " + address);
        }
        catch (UnknownHostException e)
        {
            LOGGER.error("Error resolving host", e);
            throw new IllegalArgumentException("Failed to resolve host: " + address);
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Interrupted", e);
            throw new IllegalArgumentException("Interrupted while binding to address: " + address);
        }

        this.messageHandler = messageHandler;
        this.handle = new TransportHandle(address, "tcp", UUID.randomUUID().toString());

        InetSocketAddress inAddrAnyv4 = new InetSocketAddress("0.0.0.0", uriPort);
        InetSocketAddress inAddrAnyv6 = new InetSocketAddress("::", uriPort);
        if(inAddrAnyv4.equals(inetSocketAddress) || inAddrAnyv6.equals(inetSocketAddress))
        {
            inAddrAny = true;
        }
        else
        {
            inAddrAny = false;
        }

        nettyTransportContext.addReceivingTransport(this);
    }

    @Override
    public TransportContext getTransportContext()
    {
        return nettyTransportContext;
    }

    @Override
    public void setDiscoverableTransport(DiscoverableTransport discoverableTransport)
    {
        this.discoverableTransport = discoverableTransport;
    }

    @Override
    public DiscoverableTransport getDiscoverableTransport()
    {
        return discoverableTransport;
    }

    @Override
    public TransportHandle getHandle()
    {
        return handle;
    }

    public ChannelHandler getNettyChannelHandler()
    {
        return nettyChannelHandlerAdapter;
    }

    public InetSocketAddress getInetSocketAddress()
    {
        return inetSocketAddress;
    }

    public boolean isInAddrAny()
    {
        return inAddrAny;
    }

    @Override
    public void close()
    {
        nettyTransportContext.removeReceivingTransport(this);
    }

    @Override
    public int poll(int limit)
    {
        throw new UnsupportedOperationException("This transport is not pollable");
    }

    @Override
    public boolean isPollable()
    {
        return false;
    }

    private final SimpleChannelInboundHandler<ByteBuf> nettyChannelHandlerAdapter = new SimpleChannelInboundHandler<ByteBuf>()
    {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception
        {
            DriverMessage driverMessage = tlMessage.get();
            final int length = buf.readableBytes();

            if (buf.hasArray()) {
                driverMessage.getUnsafeBuffer().putBytes(0, buf.array(), buf.arrayOffset(), length);
                driverMessage.setBufferOffset(0);
                driverMessage.setBufferLength(length);
            } else {
                driverMessage.getUnsafeBuffer().putBytes(0, buf.nioBuffer(), length);
                driverMessage.setBufferOffset(0);
                driverMessage.setBufferLength(length);

            }

            //TODO(JAF): Map header information into message metadata
            messageHandler.accept(driverMessage);
            //String contents = buf.toString(io.netty.util.CharsetUtil.US_ASCII);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error("Error receiving message {}", cause.getMessage());
        }
    };

}
