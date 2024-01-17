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

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;

import io.crate.common.CheckedFunction;
import io.crate.common.CheckedSupplier;
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
        ActionListener<Response> delegate = this;
        return new ActionListener<T>() {

            @Override
            public void onResponse(T response) {
                try {
                    delegate.onResponse(fn.apply(response));
                } catch (Throwable t) {
                    delegate.onFailure(Exceptions.toException(t));
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
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
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
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
        };
    }

    /**
     * Creates a listener that delegates all exceptions it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param bc BiConsumer invoked with delegate listener and response
     * @param <T> Type of the delegating listener's response
     * @param <R> Type of the wrapped listeners
     * @return Delegating listener
     */
    static <T, R> ActionListener<T> delegateFailure(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T r) {
                try {
                    bc.accept(delegate, r);
                } catch (Throwable t) {
                    delegate.onFailure(Exceptions.toException(t));
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
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
        return wrap(r -> runnable.run(), e -> runnable.run());
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
     * Wraps a given listener and returns a new listener which executes the provided {@code runAfter}
     * callback when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    static <Response> ActionListener<Response> runAfter(ActionListener<Response> delegate, Runnable runAfter) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    delegate.onResponse(response);
                } finally {
                    runAfter.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    delegate.onFailure(e);
                } finally {
                    runAfter.run();
                }
            }
        };
    }

    /**
     * Wraps a given listener and returns a new listener which makes sure {@link #onResponse(Object)}
     * and {@link #onFailure(Exception)} of the provided listener will be called at most once.
     */
    static <Response> ActionListener<Response> notifyOnce(ActionListener<Response> delegate) {
        return new NotifyOnceListener<>() {
            @Override
            protected void innerOnResponse(Response response) {
                delegate.onResponse(response);
            }

            @Override
            protected void innerOnFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    /**
     * Completes the given listener with the result from the provided supplier accordingly.
     * This method is mainly used to complete a listener with a block of synchronous code.
     */
    static <Response> void completeWith(ActionListener<Response> listener,
                                        CheckedSupplier<Response, ? extends Exception> supplier) {
        try {
            listener.onResponse(supplier.get());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    static <Response> ActionListener<Response> runBefore(ActionListener<Response> delegate, CheckedRunnable<?> runBefore) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    runBefore.run();
                } catch (Exception ex) {
                    delegate.onFailure(ex);
                    return;
                }
                delegate.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    runBefore.run();
                } catch (Exception ex) {
                    e.addSuppressed(ex);
                }
                delegate.onFailure(e);
            }
        };
    }

    /**
     * Creates a listener that delegates all responses it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param onFailure BiConsumer invoked with delegate listener and exception
     * @param <T> Type of the listener
     * @return Delegating listener
     */
    static <T> ActionListener<T> delegateResponse(ActionListener<T> delegate, BiConsumer<ActionListener<T>, Exception> onFailure) {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T r) {
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(delegate, e);
            }
        };
    }
}
