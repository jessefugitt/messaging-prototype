package org.kaazing.messaging.common.collections;


import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * A dynamic array that can be used concurrently with a single writer and many readers. All operations are lock-free.
 */
@SuppressWarnings("unchecked")
public class AtomicArray<T> implements Collection<T>
{
    private static final Object[] EMPTY_ARRAY = new Object[0];

    private volatile Object[] arrayRef = EMPTY_ARRAY;

    @FunctionalInterface
    public interface ToIntLimitedFunction<T>
    {
        /**
         * Applies this function to the given argument.
         *
         * @param value the function argument
         * @param limit to the number of sub actions that can be performed.
         * @return a value to indicate the number of actions that have occurred.
         */
        int apply(T value, int limit);
    }

    /**
     * Return the given element of the array
     *
     * @param index of the element to return
     * @return the element
     */
    public T get(final int index)
    {
        return (T)arrayRef[index];
    }

    /**
     * Return the array reference to allow direct iteration.
     *
     * @return the array reference
     */
    public T[] array()
    {
        return (T[])arrayRef;
    }

    /**
     * Find the first element that matches via a supplied {@link java.util.function.Predicate} function.
     *
     * @param function to match on.
     * @return the first element to match or null if no matches.
     */
    public T findFirst(final Predicate<T> function)
    {
        final T[] array = (T[])arrayRef;

        for (final T e : array)
        {
            if (function.test(e))
            {
                return e;
            }
        }

        return null;
    }

    /**
     * Iterate over each element applying a supplied action
     * <p>
     * Allocation-free if action doesn't capture.
     *
     * @param action to be applied to each element.
     */
    public void forEach(final Consumer<? super T> action)
    {
        final T[] array = (T[])arrayRef;

        for (final T e : array)
        {
            action.accept(e);
        }
    }

    /**
     * For each element, call a function to perform an action passing the element.
     * <p>
     * The count of resulting changes is returned, which can be greater than the number of elements if actions
     * are recursive.
     * <p>
     * Allocation-free if action doesn't capture.
     *
     * @param action to call and pass each element to
     * @return the number of actions that have been applied.
     */
    public int doAction(final ToIntFunction<? super T> action)
    {
        return doAction(0, action);
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
     * @return the number of actions that have been applied.
     */
    public int doAction(int fromIndex, final ToIntFunction<? super T> action)
    {
        final T[] array = (T[])arrayRef;
        final int length = array.length;
        if (length == 0)
        {
            return 0;
        }

        fromIndex = adjustForOverrun(fromIndex, length);

        int actionCount = 0;
        int i = fromIndex;
        do
        {
            actionCount += action.applyAsInt(array[i]);

            if (++i == length)
            {
                i = 0;
            }
        }
        while (i != fromIndex);

        return actionCount;
    }

    /**
     * For each element from an index, call a function to perform an action on the element.
     * <p>
     * The count of resulting changes is returned, which can be greater than the number of elements if actions
     * are recursive.
     *
     * @param fromIndex        the index to fromIndex iterating at
     * @param actionCountLimit up to which processing should occur then stop.
     * @param action           to be applied to each element
     * @return the number of actions that have been applied.
     */
    public int doLimitedAction(int fromIndex, final int actionCountLimit, final ToIntLimitedFunction<? super T> action)
    {
        final T[] array = (T[])arrayRef;
        final int length = array.length;
        if (length == 0)
        {
            return 0;
        }

        fromIndex = adjustForOverrun(fromIndex, length);

        int actionCount = 0;
        int i = fromIndex;
        do
        {
            if (actionCount >= actionCountLimit)
            {
                break;
            }

            actionCount += action.apply(array[i], actionCountLimit - actionCount);

            if (++i == length)
            {
                i = 0;
            }
        }
        while (i != fromIndex);

        return actionCount;
    }

    /**
     * Add given element to the array atomically.
     *
     * @param element to be added
     */
    public boolean add(final T element)
    {
        if (null == element)
        {
            throw new NullPointerException("element cannot be null");
        }

        arrayRef = append(arrayRef, element);

        return true;
    }

    /**
     * Remove given element from the array atomically.
     *
     * @param element to be removed
     */
    public boolean remove(final Object element)
    {
        return remove(element::equals) != null;
    }

    /**
     * Remove an element if it matches a predicate
     *
     * @param predicate to check against
     * @return the element removed or null if nothing was removed
     */
    public T remove(final Predicate<T> predicate)
    {
        final T[] oldArray = (T[])arrayRef;

        if (oldArray == EMPTY_ARRAY)
        {
            return null;
        }

        if (oldArray.length == 1)
        {
            final T element = oldArray[0];
            if (predicate.test(element))
            {
                arrayRef = EMPTY_ARRAY;
                return element;
            }
            else
            {
                return null;
            }
        }

        final int index = find(oldArray, predicate);
        if (-1 == index)
        {
            return null;
        }

        final Object[] newArray = new Object[oldArray.length - 1];
        System.arraycopy(oldArray, 0, newArray, 0, index);
        System.arraycopy(oldArray, index + 1, newArray, index, newArray.length - index);

        arrayRef = newArray;
        return oldArray[index];
    }

    public int size()
    {
        return arrayRef.length;
    }

    public Iterator<T> iterator()
    {
        return new ArrayIterator<>((T[])arrayRef);
    }

    private static final class ArrayIterator<T> implements Iterator<T>
    {
        private final T[] array;
        private int index;

        private ArrayIterator(final T[] array)
        {
            this.array = array;
        }

        public boolean hasNext()
        {
            return index < array.length;
        }

        public T next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }

            return array[index++];
        }
    }

    public boolean isEmpty()
    {
        return arrayRef.length == 0;
    }

    public boolean contains(final Object o)
    {
        return -1 != find((T[])arrayRef, o);
    }

    public Object[] toArray()
    {
        final Object[] theArray = arrayRef;
        return Arrays.copyOf(theArray, theArray.length);
    }

    public void clear()
    {
        arrayRef = EMPTY_ARRAY;
    }

    public String toString()
    {
        return "AtomicArray{" +
                "arrayRef=" + Arrays.toString(arrayRef) +
                '}';
    }

    private static int adjustForOverrun(int index, final int length)
    {
        if (index >= length)
        {
            index = length - 1;
        }

        return index;
    }

    private int find(final T[] array, final Object item)
    {
        return find(array, item::equals);
    }

    private int find(final T[] array, final Predicate<T> item)
    {
        for (int i = 0; i < array.length; i++)
        {
            if (item.test(array[i]))
            {
                return i;
            }
        }

        return -1;
    }

    private static Object[] append(final Object[] array, final Object newElement)
    {
        final Object[] newArray = Arrays.copyOf(array, array.length + 1);
        newArray[array.length] = newElement;

        return newArray;
    }

    public <E> E[] toArray(final E[] a)
    {
        return (E[])toArray();
    }

    public boolean removeAll(final Collection<?> collection)
    {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<?> collection)
    {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> collection)
    {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(final Collection<? extends T> collection)
    {
        throw new UnsupportedOperationException();
    }
}