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

import static org.elasticsearch.ExceptionsHelper.maybeError;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.codec.DecoderException;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ExceptionsHelperTests extends ESTestCase {

    @Test
    public void testMaybeError() {
        final Error outOfMemoryError = new OutOfMemoryError();
        assertError(outOfMemoryError, outOfMemoryError);

        final DecoderException decoderException = new DecoderException(outOfMemoryError);
        assertError(decoderException, outOfMemoryError);

        final Exception e = new Exception();
        e.addSuppressed(decoderException);
        assertError(e, outOfMemoryError);

        final int depth = randomIntBetween(1, 16);
        Throwable cause = new Exception();
        boolean fatal = false;
        Error error = null;
        for (int i = 0; i < depth; i++) {
            final int length = randomIntBetween(1, 4);
            for (int j = 0; j < length; j++) {
                if (!fatal && rarely()) {
                    error = new Error();
                    cause.addSuppressed(error);
                    fatal = true;
                } else {
                    cause.addSuppressed(new Exception());
                }
            }
            if (!fatal && rarely()) {
                cause = error = new Error(cause);
                fatal = true;
            } else {
                cause = new Exception(cause);
            }
        }
        if (fatal) {
            assertError(cause, error);
        } else {
            assertFalse(maybeError(cause).isPresent());
        }
        assertFalse(maybeError(new Exception(new DecoderException())).isPresent());
    }

    private void assertError(final Throwable cause, final Error error) {
        final Optional<Error> maybeError = maybeError(cause);
        assertTrue(maybeError.isPresent());
        assertThat(maybeError.get(), equalTo(error));
    }

    @Test
    public void testUnwrapCorruption() {
        Throwable corruptIndexException = new CorruptIndexException("corrupt", "resource");
        assertThat(ExceptionsHelper.unwrapCorruption(corruptIndexException), equalTo(corruptIndexException));

        Throwable corruptionAsCause = new RuntimeException(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionAsCause), equalTo(corruptIndexException));

        Throwable corruptionSuppressed = new RuntimeException();
        corruptionSuppressed.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressed), equalTo(corruptIndexException));

        Throwable corruptionSuppressedOnCause = new RuntimeException(new RuntimeException());
        corruptionSuppressedOnCause.getCause().addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressedOnCause), equalTo(corruptIndexException));

        Throwable corruptionCauseOnSuppressed = new RuntimeException();
        corruptionCauseOnSuppressed.addSuppressed(new RuntimeException(corruptIndexException));
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionCauseOnSuppressed), equalTo(corruptIndexException));

        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException()), nullValue());
        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException(new RuntimeException())), nullValue());

        Throwable withSuppressedException = new RuntimeException();
        withSuppressedException.addSuppressed(new RuntimeException());
        assertThat(ExceptionsHelper.unwrapCorruption(withSuppressedException), nullValue());
    }


    @Test
    public void testSuppressedCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException();
        e1.addSuppressed(e2);
        e2.addSuppressed(e1);
        ExceptionsHelper.unwrapCorruption(e1);

        final CorruptIndexException corruptIndexException = new CorruptIndexException("corrupt", "resource");
        RuntimeException e3 = new RuntimeException(corruptIndexException);
        e3.addSuppressed(e1);
        assertThat(ExceptionsHelper.unwrapCorruption(e3), equalTo(corruptIndexException));

        RuntimeException e4 = new RuntimeException(e1);
        e4.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(e4), equalTo(corruptIndexException));
    }

    @Test
    public void testCauseCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException(e1);
        e1.initCause(e2);
        ExceptionsHelper.unwrap(e1, IOException.class);
        ExceptionsHelper.unwrapCorruption(e1);
    }
}
