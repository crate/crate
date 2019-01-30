/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public final class ExceptionsHelper {

    private static final Logger logger = Loggers.getLogger(ExceptionsHelper.class);

    public static RuntimeException convertToRuntime(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new ElasticsearchException(e);
    }

    public static ElasticsearchException convertToElastic(Exception e) {
        if (e instanceof ElasticsearchException) {
            return (ElasticsearchException) e;
        }
        return new ElasticsearchException(e);
    }

    public static RestStatus status(Throwable t) {
        if (t != null) {
            if (t instanceof ElasticsearchException) {
                return ((ElasticsearchException) t).status();
            } else if (t instanceof IllegalArgumentException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof EsRejectedExecutionException) {
                return RestStatus.TOO_MANY_REQUESTS;
            }
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    public static Throwable unwrapCause(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof ElasticsearchWrapperException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                // dear god, if we got more than 10 levels down, WTF? just bail
                logger.warn("Exception cause unwrapping ran for 10 levels...", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    /**
     * @deprecated Don't swallow exceptions, allow them to propagate.
     */
    @Deprecated
    public static String detailedMessage(Throwable t) {
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

    /**
     * Throws a runtime exception with all given exceptions added as suppressed.
     * If the given list is empty no exception is thrown
     */
    public static <T extends Throwable> void maybeThrowRuntimeAndSuppress(List<T> exceptions) {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw new ElasticsearchException(main);
        }
    }

    public static <T extends Throwable> T useOrSuppress(T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }

    public static IOException unwrapCorruption(Throwable t) {
        return (IOException) unwrap(t, CorruptIndexException.class,
                                       IndexFormatTooOldException.class,
                                       IndexFormatTooNewException.class);
    }

    public static Throwable unwrap(Throwable t, Class<?>... clazzes) {
        if (t != null) {
            do {
                for (Class<?> clazz : clazzes) {
                    if (clazz.isInstance(t)) {
                        return t;
                    }
                }
            } while ((t = t.getCause()) != null);
        }
        return null;
    }

    /**
     * Throws the specified exception. If null if specified then <code>true</code> is returned.
     */
    public static boolean reThrowIfNotNull(@Nullable Throwable e) {
        if (e != null) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    static final int MAX_ITERATIONS = 1024;

    /**
     * Unwrap the specified throwable looking for any suppressed errors or errors as a root cause of the specified throwable.
     *
     * @param cause the root throwable
     * @return an optional error if one is found suppressed or a root cause in the tree rooted at the specified throwable
     */
    public static Optional<Error> maybeError(final Throwable cause, final Logger logger) {
        // early terminate if the cause is already an error
        if (cause instanceof Error) {
            return Optional.of((Error) cause);
        }

        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        int iterations = 0;
        while (queue.isEmpty() == false) {
            iterations++;
            // this is a guard against deeply nested or circular chains of exceptions
            if (iterations > MAX_ITERATIONS) {
                logger.warn("giving up looking for fatal errors", cause);
                break;
            }
            final Throwable current = queue.remove();
            if (current instanceof Error) {
                return Optional.of((Error) current);
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return Optional.empty();
    }

    /**
     * If the specified cause is an unrecoverable error, this method will rethrow the cause on a separate thread so that it can not be
     * caught and bubbles up to the uncaught exception handler. Note that the cause tree is examined for any {@link Error}. See
     * {@link #maybeError(Throwable, Logger)} for the semantics.
     *
     * @param throwable the throwable to possibly throw on another thread
     */
    public static void maybeDieOnAnotherThread(final Throwable throwable) {
        ExceptionsHelper.maybeError(throwable, logger).ifPresent(error -> {
            /*
             * Here be dragons. We want to rethrow this so that it bubbles up to the uncaught exception handler. Yet, sometimes the stack
             * contains statements that catch any throwable (e.g., Netty, and the JDK futures framework). This means that a rethrow here
             * will not bubble up to where we want it to. So, we fork a thread and throw the exception from there where we are sure the
             * stack does not contain statements that catch any throwable. We do not wrap the exception so as to not lose the original cause
             * during exit.
             */
            try {
                // try to log the current stack trace
                final String formatted = ExceptionsHelper.formatStackTrace(Thread.currentThread().getStackTrace());
                logger.error("fatal error\n{}", formatted);
            } finally {
                new Thread(
                        () -> {
                            throw error;
                        })
                        .start();
            }
        });
    }

    /**
     * Deduplicate the failures by exception message and index.
     */
    public static ShardOperationFailedException[] groupBy(ShardOperationFailedException[] failures) {
        List<ShardOperationFailedException> uniqueFailures = new ArrayList<>();
        Set<GroupBy> reasons = new HashSet<>();
        for (ShardOperationFailedException failure : failures) {
            GroupBy reason = new GroupBy(failure);
            if (reasons.contains(reason) == false) {
                reasons.add(reason);
                uniqueFailures.add(failure);
            }
        }
        return uniqueFailures.toArray(new ShardOperationFailedException[0]);
    }

    private static class GroupBy {
        final String reason;
        final String index;
        final Class<? extends Throwable> causeType;

        GroupBy(ShardOperationFailedException failure) {
            Throwable cause = failure.getCause();
            //the index name from the failure contains the cluster alias when using CCS. Ideally failures should be grouped by
            //index name and cluster alias. That's why the failure index name has the precedence over the one coming from the cause,
            //which does not include the cluster alias.
            String indexName = failure.index();
            if (indexName == null) {
                if (cause instanceof ElasticsearchException) {
                    final Index index = ((ElasticsearchException) cause).getIndex();
                    if (index != null) {
                        indexName = index.getName();
                    }
                }
            }
            this.index = indexName;
            this.reason = cause.getMessage();
            this.causeType = cause.getClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupBy groupBy = (GroupBy) o;
            return Objects.equals(reason, groupBy.reason) &&
                Objects.equals(index, groupBy.index) &&
                Objects.equals(causeType, groupBy.causeType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, index, causeType);
        }
    }
}
