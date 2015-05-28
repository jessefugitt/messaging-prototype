package org.kaazing.messaging.driver.command;

import org.kaazing.messaging.common.command.Command;
import org.kaazing.messaging.driver.transport.SendingTransport;

import java.util.function.Consumer;

public class DriverCommand implements Command
{
    public static final int TYPE_ADD_SENDING_TRANSPORT = 1;
    public static final int TYPE_REMOVE_AND_CLOSE_SENDING_TRANSPORT = 2;
    public static final int TYPE_ADD_RECEIVING_TRANSPORT = 3;
    public static final int TYPE_REMOVE_AND_CLOSE_RECEIVING_TRANSPORT = 4;

    private final int type;
    private int messageProducerIndex;

    public DriverCommand(int type)
    {
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    private SendingTransport sendingTransport;
    private Consumer<DriverCommand> commandCompletedAction;

    public void setSendingTransport(SendingTransport sendingTransport)
    {
        this.sendingTransport = sendingTransport;
    }

    public SendingTransport getSendingTransport()
    {
        return sendingTransport;
    }

    public void setCommandCompletedAction(Consumer<DriverCommand> commandCompletedAction)
    {
        this.commandCompletedAction = commandCompletedAction;
    }

    public Consumer<DriverCommand> getCommandCompletedAction()
    {
        return commandCompletedAction;
    }

    public int getMessageProducerIndex() {
        return messageProducerIndex;
    }

    public void setMessageProducerIndex(int messageProducerIndex) {
        this.messageProducerIndex = messageProducerIndex;
    }
}
