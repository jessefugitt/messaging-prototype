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
package org.kaazing.messaging.common.transport;

import java.util.Map;
import java.util.HashMap;

public class TransportHandle
{
    public enum Type
    {
        Aeron, AMQP
    }

    private final Type type;
    private final String id;
    private final String physicalAddress;

    public TransportHandle(String physicalAddress, Type type, String id)
    {
        this.id = id;
        this.physicalAddress = physicalAddress;
        this.type = type;
    }

    public String getPhysicalAddress()
    {
        return physicalAddress;
    }

    public Type getType()
    {
        return type;
    }

    public String getId()
    {
        return id;
    }
}
