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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

public class ExceptionsTest {

    private void assertError(final Throwable cause, final Error error) {
        final Optional<Error> maybeError = Exceptions.maybeError(cause);
        assertThat(maybeError).isPresent();
        assertThat(maybeError.get()).isEqualTo(error);
    }

    @Test
    public void testMaybeError() {
        final Error outOfMemoryError = new OutOfMemoryError();
        assertError(outOfMemoryError, outOfMemoryError);

        final IllegalArgumentException illegalArgument = new IllegalArgumentException(outOfMemoryError);
        assertError(illegalArgument, outOfMemoryError);

        final Exception e = new Exception();
        e.addSuppressed(illegalArgument);
        assertError(e, outOfMemoryError);
    }

    @Test
    public void testMaybeErrorWithMultipleSuppressedExceptinon() {
        Throwable cause = new Exception();
        cause.addSuppressed(new Exception());
        cause.addSuppressed(new Exception());
        cause.addSuppressed(new Exception());
        cause = new Exception(cause);
        assertThat(Exceptions.maybeError(cause)).isNotPresent();
    }

    @Test
    public void testMaybeErrorWithMultipleSuppressedAndError() {
        Throwable cause = new Exception();
        Error error = new Error();
        cause.addSuppressed(error);
        cause.addSuppressed(new Exception());
        cause.addSuppressed(new Exception());
        cause = new Exception(cause);
        assertError(cause, error);
    }
}
