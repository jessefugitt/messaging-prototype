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
package org.kaazing.messaging.common.collections;

import uk.co.real_logic.agrona.concurrent.AtomicArray;

import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;

public class AtomicArrayWithArg<T, A> extends AtomicArray<T>
{
    /**
     * For each element from an index, call a function an action on an element.
     * <p>
     * The count of resulting changes is returned, which can be greater than the number of elements if actions
     * are recursive.
     * <p>
     * Allocation-free if action doesn't capture.
     *
     * @param fromIndex the index to fromIndex iterating at
     * @param action    to call and pass each element to
     * @param arg       the arg to pass to the action
     * @return the number of actions that have been applied.
     */
    public int doActionWithArg(int fromIndex, final ToLongBiFunction<? super T, A> action, final A arg)
    {
        final T[] array = (T[]) super.arrayRef();
        final int length = array.length;
        if (length == 0)
        {
            return 0;
        }

        if (fromIndex >= length)
        {
            fromIndex = length - 1;
        }

        int actionCount = 0;
        int i = fromIndex;
        do
        {
            actionCount += action.applyAsLong(array[i], arg);

            if (++i == length)
            {
                i = 0;
            }
        }
        while (i != fromIndex);

        return actionCount;
    }

    /**
     * For each element from an index, call a function an action on an element.
     * <p>
     * The count of resulting changes is returned, which can be greater than the number of elements if actions
     * are recursive.
     * <p>
     * Allocation-free if action doesn't capture.
     *
     * @param fromIndex the index to fromIndex iterating at
     * @param action    to call and pass each element to
     * @param arg       the arg to pass to the action
     * @return true if the actions completed without errors or false if any errors were encountered
     */
    public boolean doActionWithArgToBoolean(int fromIndex, final ToLongBiFunction<? super T, A> action, final A arg)
    {
        boolean result = true;
        final T[] array = (T[]) super.arrayRef();
        final int length = array.length;
        if (length > 0)
        {
            if (fromIndex >= length)
            {
                fromIndex = length - 1;
            }


            int i = fromIndex;
            do
            {
                if(action.applyAsLong(array[i], arg) < 0)
                {
                    result = false;
                }
                if (++i == length)
                {
                    i = 0;
                }
            }
            while (i != fromIndex);
        }
        else
        {
            //length of 0 indicates no actions could actually be applied which is treated as an error
            result = false;
        }
        return result;
    }
}

