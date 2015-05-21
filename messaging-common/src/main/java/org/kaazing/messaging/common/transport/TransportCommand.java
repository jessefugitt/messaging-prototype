package org.kaazing.messaging.common.transport;

import org.kaazing.messaging.common.discovery.DiscoveryEvent;

import java.util.function.Consumer;

public class TransportCommand {
    public static final int TYPE_ADD_SENDING_TRANSPORT = 1;
    public static final int TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT = 2;
    public static final int TYPE_ADD_RECEIVING_TRANSPORT = 3;
    public static final int TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT = 4;
    public static final int TYPE_ADD_DISCOVERY_LISTENER = 5;
    public static final int TYPE_REMOVE_DISCOVERY_LISTENER = 6;

    private int type;
    private SendingTransport sendingTransport;
    private ReceivingTransport receivingTransport;
    private Consumer<TransportCommand> commandCompletedAction;
    private String discoveryKey;
    private Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction;

    public void setType(int type)
    {
        this.type = type;
    }

    public int getType()
    {
        return type;
    }

    public void setSendingTransport(SendingTransport sendingTransport)
    {
        this.sendingTransport = sendingTransport;
    }

    public SendingTransport getSendingTransport()
    {
        return sendingTransport;
    }

    public void setReceivingTransport(ReceivingTransport receivingTransport)
    {
        this.receivingTransport = receivingTransport;
    }

    public ReceivingTransport getReceivingTransport()
    {
        return receivingTransport;
    }

    public void setCommandCompletedAction(Consumer<TransportCommand> commandCompletedAction)
    {
        this.commandCompletedAction = commandCompletedAction;
    }

    public Consumer<TransportCommand> getCommandCompletedAction()
    {
        return commandCompletedAction;
    }


    public void setDiscoveryKey(String discoveryKey)
    {
        this.discoveryKey = discoveryKey;
    }

    public String getDiscoveryKey()
    {
        return discoveryKey;
    }

    public void setDiscoveredTransportsAction(Consumer<DiscoveryEvent<DiscoverableTransport>> discoveredTransportsAction)
    {
        this.discoveredTransportsAction = discoveredTransportsAction;
    }

    public Consumer<DiscoveryEvent<DiscoverableTransport>> getDiscoveredTransportsAction()
    {
        return discoveredTransportsAction;
    }

}
