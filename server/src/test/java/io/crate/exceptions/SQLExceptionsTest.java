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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Test;

import io.crate.common.exceptions.Exceptions;

public class SQLExceptionsTest {

    @Test
    public void testUnwrap() {
        String msg = "cannot cast";
        Throwable t = new ExecutionException(new ClassCastException(msg));
        Throwable unwrapped = SQLExceptions.unwrap(t);
        assertThat(unwrapped).isExactlyInstanceOf(ClassCastException.class);
        assertThat(unwrapped.getMessage()).isEqualTo(msg);
    }

    @Test
    public void testSuppressedCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException();
        e1.addSuppressed(e2);
        e2.addSuppressed(e1);
        SQLExceptions.unwrapCorruption(e1);

        final CorruptIndexException corruptIndexException = new CorruptIndexException("corrupt", "resource");
        RuntimeException e3 = new RuntimeException(corruptIndexException);
        e3.addSuppressed(e1);
        assertThat(SQLExceptions.unwrapCorruption(e3)).isEqualTo(corruptIndexException);

        RuntimeException e4 = new RuntimeException(e1);
        e4.addSuppressed(corruptIndexException);
        assertThat(SQLExceptions.unwrapCorruption(e4)).isEqualTo(corruptIndexException);
    }

    @Test
    public void testUnwrapCorruption() {
        Throwable corruptIndexException = new CorruptIndexException("corrupt", "resource");
        assertThat(SQLExceptions.unwrapCorruption(corruptIndexException)).isEqualTo(corruptIndexException);

        Throwable corruptionAsCause = new RuntimeException(corruptIndexException);
        assertThat(SQLExceptions.unwrapCorruption(corruptionAsCause)).isEqualTo(corruptIndexException);

        Throwable corruptionSuppressed = new RuntimeException();
        corruptionSuppressed.addSuppressed(corruptIndexException);
        assertThat(SQLExceptions.unwrapCorruption(corruptionSuppressed)).isEqualTo(corruptIndexException);

        Throwable corruptionSuppressedOnCause = new RuntimeException(new RuntimeException());
        corruptionSuppressedOnCause.getCause().addSuppressed(corruptIndexException);
        assertThat(SQLExceptions.unwrapCorruption(corruptionSuppressedOnCause)).isEqualTo(corruptIndexException);

        Throwable corruptionCauseOnSuppressed = new RuntimeException();
        corruptionCauseOnSuppressed.addSuppressed(new RuntimeException(corruptIndexException));
        assertThat(SQLExceptions.unwrapCorruption(corruptionCauseOnSuppressed)).isEqualTo(corruptIndexException);

        assertThat(SQLExceptions.unwrapCorruption(new RuntimeException())).isNull();
        assertThat(SQLExceptions.unwrapCorruption(new RuntimeException(new RuntimeException()))).isNull();

        Throwable withSuppressedException = new RuntimeException();
        withSuppressedException.addSuppressed(new RuntimeException());
        assertThat(SQLExceptions.unwrapCorruption(withSuppressedException)).isNull();
    }


    @Test
    public void testCauseCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException(e1);
        e1.initCause(e2);
        Class<?>[] clazzes = { IOException.class };
        Exceptions.unwrap(e1, clazzes);
        SQLExceptions.unwrapCorruption(e1);
    }
}
