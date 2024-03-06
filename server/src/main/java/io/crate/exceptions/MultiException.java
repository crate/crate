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

package io.crate.exceptions;

import io.crate.common.SuppressForbidden;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * Collects multiple {@code Throwable}s into one exception.
 */
public class MultiException extends RuntimeException {

    private static final int MAX_EXCEPTION_LENGTH = 10_000;
    private final Collection<? extends Throwable> exceptions;

    public MultiException(Collection<? extends Throwable> exceptions) {
        this.exceptions = exceptions;
    }

    @SuppressForbidden(reason = "Throwable#printStackTrace")
    @Override
    public void printStackTrace() {
        for (Throwable exception : exceptions) {
            exception.printStackTrace();
        }
    }

    @Override
    public void printStackTrace(PrintStream s) {
        for (Throwable exception : exceptions) {
            exception.printStackTrace(s);
        }
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        for (Throwable exception : exceptions) {
            exception.printStackTrace(s);
        }
    }

    @Override
    public String getMessage() {
        StringJoiner joiner = new StringJoiner("\n");
        for (Throwable e : exceptions) {
            joiner.add(e.getMessage());
            // Reduce the amount of characters returned to consume less memory
            // this could escalate on a huge amount of exceptions that are raised by affected shards in the cluster
            if (joiner.length() > MAX_EXCEPTION_LENGTH) {
                joiner.add("too much output. output truncated.");
                break;
            }
        }
        return joiner.toString();
    }

    @VisibleForTesting
    Collection<? extends Throwable> getExceptions() {
        return exceptions;
    }

    public static MultiException of(Throwable prevError, Throwable newError) {
        if (prevError instanceof MultiException) {
            // Create a flattened MultiException(t1, t2, t3, ...) instead of stacking them
            // to be memory efficient
            Collection<? extends Throwable> prevExceptions = ((MultiException) prevError).getExceptions();
            ArrayList<Throwable> errors = new ArrayList<>(prevExceptions.size() + 1);
            errors.addAll(prevExceptions);
            errors.add(newError);
            return new MultiException(errors);
        }
        return new MultiException(List.of(prevError, newError));
    }
}
