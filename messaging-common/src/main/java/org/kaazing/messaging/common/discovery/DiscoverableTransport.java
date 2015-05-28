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
package org.kaazing.messaging.common.discovery;

import org.kaazing.messaging.common.discovery.Discoverable;
import org.kaazing.messaging.common.transport.TransportHandle;

public class DiscoverableTransport implements Discoverable {
    private String key;
    private TransportHandle transportHandle;

    public DiscoverableTransport(String key, TransportHandle transportHandle)
    {
        this.key = key;
        this.transportHandle = transportHandle;
    }

    public TransportHandle getTransportHandle()
    {
        return transportHandle;
    }

    @Override
    public String getKey()
    {
        return key;
    }
}
