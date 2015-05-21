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

import java.util.ArrayList;
import java.util.List;

public class DiscoveryEvent<T>
{
    private final List<T> added = new ArrayList<T>();
    private final List<T> updated = new ArrayList<T>();
    private final List<T> removed = new ArrayList<T>();

    public DiscoveryEvent<T> clear() {
        added.clear();
        updated.clear();
        removed.clear();
        return this;
    }

    public List<T> getAdded()
    {
        return added;
    }

    public List<T> getUpdated()
    {
        return updated;
    }

    public List<T> getRemoved()
    {
        return removed;
    }
}
