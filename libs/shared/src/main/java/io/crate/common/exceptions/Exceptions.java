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

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public final class Exceptions {

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
