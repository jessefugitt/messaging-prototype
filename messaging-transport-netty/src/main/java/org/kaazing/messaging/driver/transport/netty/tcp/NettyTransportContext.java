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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NettyTransportContext implements TransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTransportContext.class);

    static final boolean USE_SSL = System.getProperty("org.kaazing.messaging.driver.netty.tcp.ssl") != null;

    private final SslContext serverSslCtx;
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup serverBossGroup;
    private final EventLoopGroup serverWorkerGroup;
    private final ArrayList<NettyReceivingTransport> serverReceivingTransports = new ArrayList<NettyReceivingTransport>();
    private final ReadWriteLock serverReceivingTransportsLock = new ReentrantReadWriteLock();

    private final SslContext clientSslCtx;

    //TODO(JAF): Change this to be an interface scan instead of localhost
    //public static final String DEFAULT_NETTY_SUBSCRIPTION_ADDRESS = "netty://127.0.0.1:7686";
    private final Bootstrap bootstrap;
    private EventLoopGroup group;

    public NettyTransportContext()
    {
        super();


        if (USE_SSL) {
            SelfSignedCertificate ssc = null;
            try {
                ssc = new SelfSignedCertificate();
                serverSslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
                clientSslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } catch (CertificateException e) {
                LOGGER.error("CertificateException", e);
                throw new IllegalArgumentException("Error creating transport context", e);
            } catch (SSLException e) {
                LOGGER.error("SSLException", e);
                throw new IllegalArgumentException("Error creating transport context", e);
            }
        } else {
            serverSslCtx = null;
            clientSslCtx = null;
        }

        // Configure the server.
        serverBossGroup = new NioEventLoopGroup(1);
        serverWorkerGroup = new NioEventLoopGroup();

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(serverBossGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        final ChannelPipeline p = ch.pipeline();
                        if (serverSslCtx != null) {
                            p.addLast(serverSslCtx.newHandler(ch.alloc()));
                        }
                        serverReceivingTransportsLock.readLock().lock();
                        try
                        {
                            serverReceivingTransports.forEach((nettyReceivingTransport) -> {
                                if(ch.localAddress().equals(nettyReceivingTransport.getInetSocketAddress()))
                                {
                                    p.addLast(nettyReceivingTransport.getNettyChannelHandler());
                                }
                            });
                        }
                        finally {
                            serverReceivingTransportsLock.readLock().unlock();
                        }

                    }
                });




        bootstrap = new Bootstrap();
        group = new NioEventLoopGroup();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        if (clientSslCtx != null) {
                            p.addLast(clientSslCtx.newHandler(ch.alloc()));
                        }
                    }
                });
    }

    protected Bootstrap getBootstrap() {
        return bootstrap;
    }
    protected ServerBootstrap getServerBootstrap() {
        return serverBootstrap;
    }

    public void addReceivingTransport(NettyReceivingTransport receivingTransport)
    {
        serverReceivingTransportsLock.writeLock().lock();
        try
        {
            if(!serverReceivingTransports.contains(receivingTransport))
            {
                serverReceivingTransports.add(receivingTransport);
            }
        }
        finally
        {
            serverReceivingTransportsLock.writeLock().unlock();
        }
    }

    public void removeReceivingTransport(NettyReceivingTransport receivingTransport)
    {
        serverReceivingTransportsLock.writeLock().lock();
        try
        {
            serverReceivingTransports.remove(receivingTransport);
        }
        finally
        {
            serverReceivingTransportsLock.writeLock().unlock();
        }
    }

    @Override
    public void close()
    {
        group.shutdownGracefully();
        serverBossGroup.shutdownGracefully();
        serverWorkerGroup.shutdownGracefully();
    }

    @Override
    public int doReceiveWork(AtomicArray<ReceivingTransport> receivingTransports) {
        return 0;
    }
}
