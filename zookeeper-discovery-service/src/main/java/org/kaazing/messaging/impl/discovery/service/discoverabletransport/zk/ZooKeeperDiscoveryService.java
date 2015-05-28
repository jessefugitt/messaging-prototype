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
package org.kaazing.messaging.impl.discovery.service.discoverabletransport.zk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.kaazing.messaging.common.discovery.DiscoveryEvent;
import org.kaazing.messaging.common.discovery.service.DiscoveryService;
import org.kaazing.messaging.common.discovery.DiscoverableTransport;
import org.kaazing.messaging.common.transport.TransportHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ZooKeeperDiscoveryService implements DiscoveryService<DiscoverableTransport>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperDiscoveryService.class);

    private static final String PATH = "/kaazing/topics";
    private final String connectionString;
    private final CuratorFramework client;
    private TreeCache cache = null;
    private final Gson gson;
    private final Map<String, AtomicArray<Consumer<DiscoveryEvent<DiscoverableTransport>>>> listenerMap = new ConcurrentHashMap<>();

    public ZooKeeperDiscoveryService(String connectionString)
    {
        this.connectionString = connectionString;
        client = CuratorFrameworkFactory.newClient(this.connectionString, new ExponentialBackoffRetry(1000, 3));
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
    }

    @Override
    public ZooKeeperDiscoveryService start()
    {
        try
        {
            client.start();

            // in this example we will cache data. Notice that this is optional.
            cache = new TreeCache(client, PATH);

            //Use async POST_INITIALIZED_EVENT to do the buildling in the background
            //cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            cache.start();

            final AtomicBoolean initialized = new AtomicBoolean(false);

            TreeCacheListener listener = new TreeCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
                {
                    switch ( event.getType() )
                    {
                        case NODE_ADDED:
                        {
                            LOGGER.debug("ZKNode added {}", ZKPaths.getNodeFromPath(event.getData().getPath()));
                            String logicalName = event.getData().getPath().split("__")[1];

                            AtomicArray<Consumer<DiscoveryEvent<DiscoverableTransport>>> messageFlowListeners = listenerMap.get(logicalName);
                            if(messageFlowListeners != null)
                            {
                                ChildData childData = event.getData();
                                byte[] bytes = childData.getData();
                                String jsonStr = new String(bytes, StandardCharsets.UTF_8);
                                TransportHandle transportHandle = gson.fromJson(jsonStr, TransportHandle.class);
                                DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);

                                DiscoveryEvent<DiscoverableTransport> discoveryEvent = new DiscoveryEvent<DiscoverableTransport>();
                                discoveryEvent.getAdded().add(discoverableTransport);
                                messageFlowListeners.forEach((listener) -> listener.accept(discoveryEvent));

                            }
                            break;
                        }

                        case NODE_UPDATED:
                        {
                            System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            String logicalName = event.getData().getPath().split("__")[1];

                            AtomicArray<Consumer<DiscoveryEvent<DiscoverableTransport>>> messageFlowListeners = listenerMap.get(logicalName);
                            if(messageFlowListeners != null)
                            {
                                ChildData childData = event.getData();
                                byte[] bytes = childData.getData();
                                String jsonStr = new String(bytes, StandardCharsets.UTF_8);
                                TransportHandle transportHandle = gson.fromJson(jsonStr, TransportHandle.class);

                                DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);

                                DiscoveryEvent<DiscoverableTransport> discoveryEvent = new DiscoveryEvent<DiscoverableTransport>();
                                discoveryEvent.getUpdated().add(discoverableTransport);
                                messageFlowListeners.forEach((listener) -> listener.accept(discoveryEvent));
                            }
                            break;
                        }

                        case NODE_REMOVED:
                        {
                            LOGGER.debug("ZKNode removed {}", ZKPaths.getNodeFromPath(event.getData().getPath()));
                            String logicalName = event.getData().getPath().split("__")[1];

                            AtomicArray<Consumer<DiscoveryEvent<DiscoverableTransport>>> messageFlowListeners = listenerMap.get(logicalName);
                            if(messageFlowListeners != null)
                            {
                                ChildData childData = event.getData();
                                byte[] bytes = childData.getData();
                                String jsonStr = new String(bytes, StandardCharsets.UTF_8);
                                TransportHandle transportHandle = gson.fromJson(jsonStr, TransportHandle.class);

                                DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);

                                DiscoveryEvent<DiscoverableTransport> discoveryEvent = new DiscoveryEvent<DiscoverableTransport>();
                                discoveryEvent.getRemoved().add(discoverableTransport);
                                messageFlowListeners.forEach((listener) -> listener.accept(discoveryEvent));
                            }
                            break;
                        }

                        case INITIALIZED:
                        {
                            LOGGER.debug("Cache initialized");
                            initialized.set(true);
                            break;
                        }

                        //TODO(JAF): Handle disconnect
                    }
                }
            };
            cache.getListenable().addListener(listener);
            while(initialized.get() != true)
            {
                LOGGER.debug("Waiting for cache to be initialized...");
                Thread.sleep(100);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public ZooKeeperDiscoveryService stop()
    {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(client);
        return this;
    }

    @Override
    public AtomicArray<DiscoverableTransport> getValues(String key)
    {
        //TODO(JAF): Add support for more than one node on a single topic
        AtomicArray<DiscoverableTransport> matches = new AtomicArray<DiscoverableTransport>();

        Map<String, ChildData> childDataMap = cache.getCurrentChildren(PATH + ZKPaths.PATH_SEPARATOR + key);
        if(childDataMap != null)
        {
            for(ChildData childData : childDataMap.values())
            {
                byte[] bytes = childData.getData();
                String jsonStr = new String(bytes, StandardCharsets.UTF_8);
                TransportHandle transportHandle = gson.fromJson(jsonStr, TransportHandle.class);
                DiscoverableTransport discoverableTransport = new DiscoverableTransport(key, transportHandle);
                matches.add(discoverableTransport);
            }
        }

        return matches;
    }

    @Override
    public void addDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener)
    {
        Collection<Consumer<DiscoveryEvent<DiscoverableTransport>>> listeners = listenerMap.computeIfAbsent(
                key, (ignore) -> new AtomicArray<Consumer<DiscoveryEvent<DiscoverableTransport>>>());

        if(!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public void removeDiscoveryEventListener(String key, Consumer<DiscoveryEvent<DiscoverableTransport>> listener)
    {
        Collection<Consumer<DiscoveryEvent<DiscoverableTransport>>> listeners = listenerMap.get(key);
        if(listeners != null)
        {
            listeners.remove(listener);
        }
    }

    @Override
    public void registerValue(String key, DiscoverableTransport value)
    {
        TransportHandle handle = value.getTransportHandle();
        //TODO(JAF): Add support for more than one node on a single topic
        String path = ZKPaths.makePath(PATH + ZKPaths.PATH_SEPARATOR + key, handle.getId() + "__" + key);
        byte[] bytes = gson.toJson(handle).getBytes(StandardCharsets.UTF_8);
        try
        {
            client.setData().forPath(path, bytes);
        }
        catch (KeeperException.NoNodeException e)
        {
            try
            {
                //Try creating the parents if needed
                client.create().creatingParentsIfNeeded().forPath(path, bytes);
            }
            catch (Exception ex)
            {
                //TODO(JAF): Handle registration exception
                ex.printStackTrace();
            }
        }
        catch (Exception ex)
        {
            //TODO(JAF): Handle registration exception
            ex.printStackTrace();
        }

    }

    @Override
    public void unregisterValue(String key, DiscoverableTransport value)
    {
        TransportHandle handle = value.getTransportHandle();
        //TODO(JAF): Add support for more than one node on a single topic
        String path = ZKPaths.makePath(PATH + ZKPaths.PATH_SEPARATOR + key, handle.getId() + "__" + key);
        try
        {
            client.delete().forPath(path);
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

}
