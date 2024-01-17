/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.common.exceptions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public final class Exceptions {

    private static final Logger LOGGER = LogManager.getLogger(Exceptions.class);

    private Exceptions() {
    }

    /**
     * Rethrow an {@link java.lang.Throwable} preserving the stack trace but making it unchecked.
     *
     * @param ex to be rethrown and unchecked.
     */
    public static void rethrowUnchecked(final Throwable ex) {
        Exceptions.rethrow(ex);
    }

    public static <T> T rethrowRuntimeException(Throwable t) {
        throw toRuntimeException(t);
    }

    public static Exception toException(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            var cause = t.getCause();
            if (cause instanceof Exception ex) {
                return ex;
            }
            // Prefer to keep CompletionException/ExecutionException as is over wrapping.
            // The CompletionException/ExecutionException have a chance of being unwrapped
            // later to get the real cause. RuntimeExceptions are never unwrapped.
            return (Exception) t;
        }
        if (t instanceof Exception e) {
            return e;
        } else {
            return new RuntimeException(t);
        }
    }

    public static RuntimeException toRuntimeException(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            t = t.getCause();
        }
        if (t instanceof RuntimeException re) {
            return re;
        } else {
            return new RuntimeException(t);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrow(final Throwable t) throws T {
        throw (T) t;
    }


    /**
     * Rethrows the first exception in the list and adds all remaining to the suppressed list.
     * If the given list is empty no exception is thrown
     *
     */
    public static <T extends Throwable> void rethrowAndSuppress(List<T> exceptions) throws T {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw main;
        }
    }

    public static <T extends Throwable> T useOrSuppress(@Nullable T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }

    public static String stackTrace(Throwable e) {
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(printWriter);
        return stackTraceStringWriter.toString();
    }

    public static String formatStackTrace(final StackTraceElement[] stackTrace) {
        return Arrays.stream(stackTrace).skip(1).map(e -> "\tat " + e).collect(Collectors.joining("\n"));
    }

    /**
     * Looks at the given Throwable and its cause(s) and returns the first Throwable that is of one of the given classes or {@code null}
     * if no matching Throwable is found. Unlike {@link #unwrapCorruption} this method does only check the given Throwable and its causes
     * but does not look at any suppressed exceptions.
     * @param t Throwable
     * @param clazzes Classes to look for
     * @return Matching Throwable if one is found, otherwise {@code null}
     */
    public static Throwable unwrap(Throwable t, Class<?>... clazzes) {
        if (t != null) {
            final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            do {
                if (seen.add(t) == false) {
                    return null;
                }
                for (Class<?> clazz : clazzes) {
                    if (clazz.isInstance(t)) {
                        return t;
                    }
                }
            } while ((t = t.getCause()) != null);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> Optional<T> unwrapCausesAndSuppressed(Throwable cause, Predicate<Throwable> predicate) {
        if (predicate.test(cause)) {
            return Optional.of((T) cause);
        }

        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (queue.isEmpty() == false) {
            final Throwable current = queue.remove();
            if (seen.add(current) == false) {
                continue;
            }
            if (predicate.test(current)) {
                return Optional.of((T) current);
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return Optional.empty();
    }

    /**
     * Unwrap the specified throwable looking for any suppressed errors or errors as a root cause of the specified throwable.
     *
     * @param cause the root throwable
     * @return an optional error if one is found suppressed or a root cause in the tree rooted at the specified throwable
     */
    public static Optional<Error> maybeError(final Throwable cause) {
        return unwrapCausesAndSuppressed(cause, t -> t instanceof Error);
    }

    /**
     * If the specified cause is an unrecoverable error, this method will rethrow the cause on a separate thread so that it can not be
     * caught and bubbles up to the uncaught exception handler. Note that the cause tree is examined for any {@link Error}. See
     * {@link #maybeError(Throwable)} for the semantics.
     *
     * @param throwable the throwable to possibly throw on another thread
     */
    public static void maybeDieOnAnotherThread(final Throwable throwable) {
        Exceptions.maybeError(throwable).ifPresent(error -> {
            /*
             * Here be dragons. We want to rethrow this so that it bubbles up to the uncaught exception handler. Yet, sometimes the stack
             * contains statements that catch any throwable (e.g., Netty, and the JDK futures framework). This means that a rethrow here
             * will not bubble up to where we want it to. So, we fork a thread and throw the exception from there where we are sure the
             * stack does not contain statements that catch any throwable. We do not wrap the exception so as to not lose the original cause
             * during exit.
             */
            try {
                // try to log the current stack trace
                final String formatted = Exceptions.formatStackTrace(Thread.currentThread().getStackTrace());
                LOGGER.error("fatal error\n{}", formatted);
            } finally {
                new Thread(() -> {
                    throw error;
                }).start();
            }
        });
    }

    /**
     * Retrieves the messages of a Throwable including its causes.
     * Should only be used for user-facing or network critical use cases.
     * Does not contain a proper stacktrace.
     * @return a String of format ExceptionName[msg];...
     */
    public static String userFriendlyMessageInclNested(Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        if (t.getCause() != null) {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getSimpleName());
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                sb.append("; ");
                t = t.getCause();
                if (t != null) {
                    sb.append("nested: ");
                }
            }
            return sb.toString();
        } else {
            return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
        }
    }

    public static String userFriendlyMessage(Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
    }
}
