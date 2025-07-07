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

package org.elasticsearch.action;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;

import io.crate.common.CheckedFunction;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

/**
 * A listener for action responses or failures.
 */
public interface ActionListener<Response> extends BiConsumer<Response, Throwable> {
    /**
     * Handle action response. This response may constitute a failure or a
     * success but it is up to the listener to make that decision.
     */
    void onResponse(Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     */
    void onFailure(Exception e);

    @Override
    default void accept(Response response, Throwable throwable) {
        if (throwable == null) {
            onResponse(response);
        } else {
            onFailure(Exceptions.toException(SQLExceptions.unwrap(throwable)));
        }
    }


    /**
     * Create a new ActionListener that maps the result using the given mapping function.
     **/
    default <T, E extends Exception> ActionListener<T> map(CheckedFunction<? super T, Response, E> fn) {
        return withOnResponse((l, response) -> l.onResponse(fn.apply(response)));
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding consumer when the response (or failure) is received.
     *
     * @param onResponse the checked consumer of the response, when the listener receives one
     * @param onFailure the consumer of the failure, when the listener receives one
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the consumer when received
     */
    static <Response> ActionListener<Response> wrap(CheckedConsumer<Response, ? extends Exception> onResponse,
                                                    Consumer<Exception> onFailure) {
        return new SimpleListener<>(onResponse, onFailure);
    }


    static class SimpleListener<T> implements ActionListener<T> {

        private final CheckedConsumer<T, ? extends Exception> onResponse;
        private final Consumer<Exception> onFailure;

        private SimpleListener(CheckedConsumer<T, ? extends Exception> onResponse,
                               Consumer<Exception> onFailure) {
            this.onResponse = onResponse;
            this.onFailure = onFailure;
        }

        @Override
        public void accept(T response, Throwable throwable) {
            if (throwable == null) {
                try {
                    onResponse.accept(response);
                    return;
                } catch (Throwable t) {
                    throwable = t;
                }
            }
            onFailure.accept(Exceptions.toException(SQLExceptions.unwrap(throwable)));
        }


        @Override
        public void onResponse(T response) {
            try {
                onResponse.accept(response);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            onFailure.accept(e);
        }
    }

    /// Return a new listener that uses the provided `onResponse` consumer instead of calling the original `onResponse`.
    /// The original listener is used on failures.
    default <T> ActionListener<T> withOnResponse(CheckedBiConsumer<ActionListener<Response>, T, Exception> onResponse) {
        ActionListener<Response> original = this;
        return new SimpleListener<>(response -> onResponse.accept(original, response), original::onFailure);
    }

    /// Return a new listener that calls `onFailure` in case of a failure.
    /// The original listener is used for `onResponse` or as fallback in case the new `onFailure` consumer fails itself.
    default ActionListener<Response> withOnFailure(CheckedBiConsumer<ActionListener<Response>, Exception, Exception> onFailure) {
        ActionListener<Response> original = this;
        return new SimpleListener<>(
            original::onResponse,
            ex -> {
                try {
                    onFailure.accept(original, ex);
                } catch (Exception onFailureEx) {
                    original.onFailure(onFailureEx);
                }
            }
        );
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding runnable when the response (or failure) is received.
     *
     * @param runnable the runnable that will be called in event of success or failure
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the runnable when received
     */
    static <Response> ActionListener<Response> wrap(Runnable runnable) {
        return new SimpleListener<>(_ -> runnable.run(), _ -> runnable.run());
    }

    /**
     * Notifies every given listener with the response passed to {@link #onResponse(Object)}. If a listener itself throws an exception
     * the exception is forwarded to {@link #onFailure(Exception)}. If in turn {@link #onFailure(Exception)} fails all remaining
     * listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onResponse(Iterable<ActionListener<Response>> listeners, Response response) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onResponse(response);
            } catch (Exception ex) {
                try {
                    listener.onFailure(ex);
                } catch (Exception ex1) {
                    exceptionList.add(ex1);
                }
            }
        }
        SQLExceptions.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Notifies every given listener with the failure passed to {@link #onFailure(Exception)}. If a listener itself throws an exception
     * all remaining listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onFailure(Iterable<ActionListener<Response>> listeners, Exception failure) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onFailure(failure);
            } catch (Exception ex) {
                exceptionList.add(ex);
            }
        }
        SQLExceptions.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Returns a new wrapped listener which executes the provided {@code runAfter}
     * callback when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    default ActionListener<Response> runAfter(Runnable runAfter) {
        ActionListener<Response> original = this;
        return new SimpleListener<>(
            response -> {
                try {
                    original.onResponse(response);
                } finally {
                    runAfter.run();
                }
            },
            err -> {
                try {
                    original.onFailure(err);
                } finally {
                    runAfter.run();
                }
            }
        );
    }

    /**
     * Returns a new wrapped listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    default ActionListener<Response> runBefore(CheckedRunnable<?> runBefore) {
        ActionListener<Response> original = this;
        return new SimpleListener<>(
            response -> {
                try {
                    runBefore.run();
                } catch (Exception ex) {
                    original.onFailure(ex);
                    return;
                }
                original.onResponse(response);
            },
            err -> {
                try {
                    runBefore.run();
                } catch (Exception ex) {
                    err.addSuppressed(ex);
                }
                original.onFailure(err);
            }
        );
    }
}
