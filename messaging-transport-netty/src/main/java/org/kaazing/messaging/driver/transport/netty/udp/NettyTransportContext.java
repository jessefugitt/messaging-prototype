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
package org.kaazing.messaging.driver.transport.netty.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.*;

public class NettyTransportContext implements TransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTransportContext.class);

    //TODO(JAF): Change this to be an interface scan instead of localhost
    //public static final String DEFAULT_NETTY_SUBSCRIPTION_ADDRESS = "netty://127.0.0.1:7686";
    private final Bootstrap bootstrap;
    private EventLoopGroup group;

    public NettyTransportContext()
    {
        super();
        bootstrap = new Bootstrap();
        group = new NioEventLoopGroup();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelHandler()
        {

            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception
            {
                LOGGER.debug("Handler added with ctx {}", ctx);
            }

            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) throws Exception
            {
                LOGGER.debug("Handler removed with ctx {}", ctx);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
            {
                LOGGER.debug("exceptionCaught {}", ctx, cause);
            }
        });
    }

    protected Bootstrap getBootstrap()
    {
        return bootstrap;
    }

    @Override
    public void close()
    {
        group.shutdownGracefully();
    }

    @Override
    public int doReceiveWork(AtomicArray<ReceivingTransport> receivingTransports) {
        return 0;
    }
}
