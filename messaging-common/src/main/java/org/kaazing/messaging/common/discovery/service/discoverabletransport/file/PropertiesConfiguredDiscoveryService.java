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
package org.kaazing.messaging.common.discovery.service.discoverabletransport.file;

import org.kaazing.messaging.common.discovery.service.discoverabletransport.basic.BasicDiscoveryService;
import org.kaazing.messaging.common.transport.DiscoverableTransport;
import org.kaazing.messaging.common.transport.TransportHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.aeron.common.uri.AeronUri;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;

public class PropertiesConfiguredDiscoveryService extends BasicDiscoveryService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfiguredDiscoveryService.class);

    private final Properties properties;
    private final String classpathFileName;
    public PropertiesConfiguredDiscoveryService(String classpathFileName) throws IOException
    {
        this.classpathFileName = classpathFileName;
        this.properties = loadPropertiesFromFile(classpathFileName);
    }

    public PropertiesConfiguredDiscoveryService(Properties properties)
    {
        this.classpathFileName = null;
        this.properties = properties;
    }
    protected Properties loadPropertiesFromFile(String fileName) throws IOException
    {
        Properties props = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream(fileName);
        if(input == null)
        {
            throw new FileNotFoundException("Properties file not found: " + fileName);
        }
        else
        {
            props.load(input);
            input.close();
        }

        return props;
    }

    protected void loadTransportHandlesFromProperties()
    {
        Enumeration e = properties.propertyNames();

        while (e.hasMoreElements())
        {
            String logicalName = (String) e.nextElement();
            String transportHandlesValue = properties.getProperty(logicalName);
            String[] transportHandles = transportHandlesValue.split(";");
            for(int i = 0; i < transportHandles.length; i++)
            {
                TransportHandle transportHandle = null;
                String transportHandleStr = transportHandles[i];
                if(transportHandleStr.startsWith("aeron"))
                {
                    AeronUri aeronUri = AeronUri.parse(transportHandleStr);


                    if(aeronUri.get("streamId") != null)
                    {
                        int streamId = Integer.parseInt(aeronUri.get("streamId"));
                    }
                    else
                    {
                        transportHandleStr += "|streamId=0";
                    }

                    transportHandle = new TransportHandle(transportHandleStr, TransportHandle.Type.Aeron, UUID.randomUUID().toString());

                    DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);
                    registerValue(logicalName, discoverableTransport);
                }
                else if(transportHandleStr.startsWith("amqp"))
                {
                    transportHandle = new TransportHandle(transportHandleStr, TransportHandle.Type.AMQP, UUID.randomUUID().toString());
                    DiscoverableTransport discoverableTransport = new DiscoverableTransport(logicalName, transportHandle);
                    registerValue(logicalName, discoverableTransport);
                }
                else
                {
                    LOGGER.warn("Unknown transport handle {}", transportHandleStr);
                }
            }
        }
    }

    @Override
    public PropertiesConfiguredDiscoveryService start()
    {
        loadTransportHandlesFromProperties();
        return this;
    }

    @Override
    public PropertiesConfiguredDiscoveryService stop()
    {
        return this;
    }

}
