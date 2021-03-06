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
package org.kaazing.messaging.driver.transport;

import org.kaazing.messaging.discovery.DiscoverableTransport;
import org.kaazing.messaging.common.transport.TransportHandle;

public interface ReceivingTransport
{
    public TransportContext getTransportContext();
    public TransportHandle getHandle();
    public void close();
    public boolean isPollable();
    public int poll(int limit);
    public void setDiscoverableTransport(DiscoverableTransport discoverableTransport);
    public DiscoverableTransport getDiscoverableTransport();
}
