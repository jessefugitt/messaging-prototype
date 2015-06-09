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
package org.kaazing.messaging.discovery.service;

import org.kaazing.messaging.common.collections.AtomicArray;
import org.kaazing.messaging.discovery.DiscoveryEvent;

import java.util.function.Consumer;

public interface DiscoveryService<T>
{
    public AtomicArray<T> getValues(String key);

    public void addDiscoveryEventListener(String key, Consumer<DiscoveryEvent<T>> listener);

    public void removeDiscoveryEventListener(String key, Consumer<DiscoveryEvent<T>> listener);

    public void registerValue(String key, T value);

    public void unregisterValue(String key, T value);

    public DiscoveryService<T> start();

    public DiscoveryService<T> stop();
}
