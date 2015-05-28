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
package org.kaazing.messaging.driver.transport.aeron;

import org.kaazing.messaging.driver.transport.ReceivingTransport;
import org.kaazing.messaging.driver.transport.TransportContext;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.concurrent.atomic.AtomicInteger;

public class AeronTransportContext implements TransportContext
{
    private final static int FRAGMENT_COUNT_LIMIT = 10;

    private final Aeron.Context aeronContext;
    private final Aeron aeron;
    private int roundRobinIndex = 0;

    public AeronTransportContext()
    {
        super();
        aeronContext = new Aeron.Context();
        aeron = Aeron.connect(aeronContext);
    }

    protected Aeron.Context getAeronContext()
    {
        return aeronContext;
    }

    protected Aeron getAeron()
    {
        return aeron;
    }

    public int doReceiveWork(AtomicArray<ReceivingTransport> receivingTransports)
    {
        if (receivingTransports.size() <= ++roundRobinIndex)
        {
            roundRobinIndex = 0;
        }
        return receivingTransports.doLimitedAction(roundRobinIndex, FRAGMENT_COUNT_LIMIT, pollAction);
    }

    @Override
    public void close()
    {
        aeron.close();
        aeronContext.close();
    }

    private final AtomicArray.ToIntLimitedFunction<ReceivingTransport> pollAction = new AtomicArray.ToIntLimitedFunction<ReceivingTransport>()
    {
        @Override
        public int apply(ReceivingTransport receivingTransport, int limit)
        {
            return receivingTransport.poll(limit);
        }
    };
}
