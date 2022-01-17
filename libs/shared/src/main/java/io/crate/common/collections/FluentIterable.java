package io.crate.common.collections;



import java.util.*;
import java.util.stream.Stream;

/**
 * A discouraged (but not deprecated) precursor to Java's superior {@link Stream} library.
 *
 * <p>The following types of methods are provided:
 *
 * <ul>
 *   <li>chaining methods which return a new {@code FluentIterable} based in some way on the
 *       contents of the current one (for example {@link #transform})
 *   <li>element extraction methods which facilitate the retrieval of certain elements (for example
 *       {@link #last})
 *   <li>query methods which answer questions about the {@code FluentIterable}'s contents (for
 *       example {@link #anyMatch})
 *   <li>conversion methods which copy the {@code FluentIterable}'s contents into a new collection
 *       or array (for example {@link #toList})
 * </ul>
 *
 * <p>Several lesser-used features are currently available only as static methods on the {@link
 * Iterables} class.
 *
 * <p><a id="streams"></a>
 *
 * <h3>Comparison to streams</h3>
 *
 * <p>{@link Stream} is similar to this class, but generally more powerful, and certainly more
 * standard. Key differences include:
 *
 * <ul>
 *   <li>A stream is <i>single-use</i>; it becomes invalid as soon as any "terminal operation" such
 *       as {@code findFirst()} or {@code iterator()} is invoked. (Even though {@code Stream}
 *       contains all the right method <i>signatures</i> to implement {@link Iterable}, it does not
 *       actually do so, to avoid implying repeat-iterability.) {@code FluentIterable}, on the other
 *       hand, is multiple-use, and does implement {@link Iterable}.
 *   <li>Streams offer many features not found here, including {@code min/max}, {@code distinct},
 *       {@code reduce}, {@code sorted}, the very powerful {@code collect}, and built-in support for
 *       parallelizing stream operations.
 *   <li>{@code FluentIterable} contains several features not available on {@code Stream}, which are
 *       noted in the method descriptions below.
 *   <li>Streams include primitive-specialized variants such as {@code IntStream}, the use of which
 *       is strongly recommended.
 *   <li>Streams are standard Java, not requiring a third-party dependency.
 * </ul>
 *
 * <h3>Example</h3>
 *
 * <p>Here is an example that accepts a list from a database call, filters it based on a predicate,
 * transforms it by invoking {@code toString()} on each element, and returns the first 10 elements
 * as a {@code List}:
 *
 * <pre>{@code
 * ImmutableList<String> results =
 *     FluentIterable.from(database.getClientList())
 *         .filter(Client::isActiveInLastMonth)
 *         .transform(Object::toString)
 *         .limit(10)
 *         .toList();
 * }</pre>
 *
 * The approximate stream equivalent is:
 *
 * <pre>{@code
 * List<String> results =
 *     database.getClientList()
 *         .stream()
 *         .filter(Client::isActiveInLastMonth)
 *         .map(Object::toString)
 *         .limit(10)
 *         .collect(Collectors.toList());
 * }</pre>
 *
 * @author Marcin Mikosik
 * @since 12.0
 */
public abstract class FluentIterable<E> implements Iterable<E> {
    // We store 'iterable' and use it instead of 'this' to allow Iterables to perform instanceof
    // checks on the _original_ iterable when FluentIterable.from is used.
    // To avoid a self retain cycle under j2objc, we store Optional.absent() instead of
    // Optional.of(this). To access the iterator delegate, call #getDelegate(), which converts to
    // absent() back to 'this'.
    private final Optional<Iterable<E>> iterableDelegate;

    /** Constructor for use by subclasses. */
    protected FluentIterable() {
        this.iterableDelegate = Optional.empty();
    }

    FluentIterable(Iterable<E> iterable) {
        Objects.requireNonNull(iterable);
        this.iterableDelegate = Optional.ofNullable(this != iterable ? iterable : null);
    }

    /**
     * Returns a fluent iterable that combines two iterables. The returned iterable has an iterator
     * that traverses the elements in {@code a}, followed by the elements in {@code b}. The source
     * iterators are not polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>{@code Stream} equivalent:</b> {@link Stream#concat}.
     *
     * @since 20.0
     */
    public static <T> FluentIterable<T> concat(Iterable<? extends T> a, Iterable<? extends T> b) {
        return concatNoDefensiveCopy(a, b);
    }

    /**
     * Returns a fluent iterable that combines three iterables. The returned iterable has an iterator
     * that traverses the elements in {@code a}, followed by the elements in {@code b}, followed by
     * the elements in {@code c}. The source iterators are not polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>{@code Stream} equivalent:</b> use nested calls to {@link Stream#concat}, or see the
     * advice in {@link #concat(Iterable...)}.
     *
     * @since 20.0
     */
    public static <T> FluentIterable<T> concat(
        Iterable<? extends T> a, Iterable<? extends T> b, Iterable<? extends T> c) {
        return concatNoDefensiveCopy(a, b, c);
    }

    /**
     * Returns a fluent iterable that combines four iterables. The returned iterable has an iterator
     * that traverses the elements in {@code a}, followed by the elements in {@code b}, followed by
     * the elements in {@code c}, followed by the elements in {@code d}. The source iterators are not
     * polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>{@code Stream} equivalent:</b> use nested calls to {@link Stream#concat}, or see the
     * advice in {@link #concat(Iterable...)}.
     *
     * @since 20.0
     */
    public static <T> FluentIterable<T> concat(
        Iterable<? extends T> a,
        Iterable<? extends T> b,
        Iterable<? extends T> c,
        Iterable<? extends T> d) {
        return concatNoDefensiveCopy(a, b, c, d);
    }

    /**
     * Returns a fluent iterable that combines several iterables. The returned iterable has an
     * iterator that traverses the elements of each iterable in {@code inputs}. The input iterators
     * are not polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>{@code Stream} equivalent:</b> to concatenate an arbitrary number of streams, use {@code
     * Stream.of(stream1, stream2, ...).flatMap(s -> s)}. If the sources are iterables, use {@code
     * Stream.of(iter1, iter2, ...).flatMap(Streams::stream)}.
     *
     * @throws NullPointerException if any of the provided iterables is {@code null}
     * @since 20.0
     */
    public static <T> FluentIterable<T> concat(Iterable<? extends T>... inputs) {
        return concatNoDefensiveCopy(Arrays.copyOf(inputs, inputs.length));
    }

    /**
     * Returns a fluent iterable that combines several iterables. The returned iterable has an
     * iterator that traverses the elements of each iterable in {@code inputs}. The input iterators
     * are not polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it. The methods of the returned iterable may throw {@code
     * NullPointerException} if any of the input iterators is {@code null}.
     *
     * <p><b>{@code Stream} equivalent:</b> {@code streamOfStreams.flatMap(s -> s)} or {@code
     * streamOfIterables.flatMap(Streams::stream)}. (See {@link Streams#stream}.)
     *
     * @since 20.0
     */
    public static <T> FluentIterable<T> concat(
        final Iterable<? extends Iterable<? extends T>> inputs) {
        Objects.requireNonNull(inputs);
        return new FluentIterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return Iterators.concat(Iterators.transform(inputs.iterator(), Iterable::iterator));
            }
        };
    }

    private static <T> FluentIterable<T> concatNoDefensiveCopy(
        final Iterable<? extends T>... inputs) {
        for (Iterable<? extends T> input : inputs) {
            Objects.requireNonNull(input);
        }
        return new FluentIterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return Iterators.concat(
                    /* lazily generate the iterators on each input only as needed */
                    new Iterators.AbstractIndexedListIterator<Iterator<? extends T>>(inputs.length) {
                        @Override
                        public Iterator<? extends T> get(int i) {
                            return inputs[i].iterator();
                        }
                    });
            }
        };
    }

    /**
     * Returns a string representation of this fluent iterable, with the format {@code [e1, e2, ...,
     * en]}.
     *
     * <p><b>{@code Stream} equivalent:</b> {@code stream.collect(Collectors.joining(", ", "[", "]"))}
     * or (less efficiently) {@code stream.collect(Collectors.toList()).toString()}.
     */
    @Override
    public String toString() {
        return iterableDelegate.toString();
    }

}
