package org.kaazing.messaging.client.message;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Properties;

public class Message {
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    private DirectBuffer buffer;
    private Properties metadata;
    private int bufferOffset;
    private int bufferLength;

    /**
     * Creates a message without allocating an internal buffer
     */
    public Message()
    {
        this.buffer = null;
        metadata = new Properties();
    }

    /**
     * Creates a message allocating an UnsafeBuffer of the passed in size
     * @param size
     */
    public Message(int size)
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
}
