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

package io.crate.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import dev.hardwood.InputFile;

public class NonClosingInputFile implements InputFile {
    private final InputFile delegate;

    NonClosingInputFile(InputFile delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        return delegate.readRange(offset, length);
    }

    @Override
    public void close() throws IOException {
        // do not close on the delegate so that the file is preserved in the cache
        // and can be re-used for repeated queries
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Optional<String> identity() throws IOException {
        return delegate.identity();
    }
}
