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
package org.kaazing.messaging.driver.message;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Properties;

public class DriverMessage
{
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    private DirectBuffer buffer;
    private Properties metadata;
    private int bufferOffset;
    private int bufferLength;

    private int messageProducerIndex;

    /**
     * Creates a message without allocating an internal buffer
     */
    public DriverMessage()
    {
        this.buffer = null;
        metadata = new Properties();
    }

    /**
     * Creates a message allocating an UnsafeBuffer of the passed in size
     * @param size
     */
    public DriverMessage(int size)
    {
        buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
        metadata = new Properties();
    }

    public DirectBuffer getBuffer()
    {
        return buffer;
    }

    public UnsafeBuffer getUnsafeBuffer()
    {
        return (UnsafeBuffer) buffer;
    }

    public void setBuffer(DirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public int getBufferOffset()
    {
        return bufferOffset;
    }

    public void setBufferOffset(int offset)
    {
        this.bufferOffset = offset;
    }

    public int getBufferLength()
    {
        return bufferLength;
    }

    public void setBufferLength(int length)
    {
        this.bufferLength = length;
    }

    public Properties getMetadata()
    {
        return metadata;
    }

    public void setMessageProducerIndex(int messageProducerIndex)
    {
        this.messageProducerIndex = messageProducerIndex;
    }

    public int getMessageProducerIndex()
    {
        return messageProducerIndex;
    }
}
