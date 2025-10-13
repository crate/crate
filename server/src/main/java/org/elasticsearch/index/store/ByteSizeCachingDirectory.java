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

package org.elasticsearch.index.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.function.Supplier;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;

import io.crate.common.Suppliers;
import io.crate.common.unit.TimeValue;

final class ByteSizeCachingDirectory extends FilterDirectory {

    private static long estimateSizeInBytes(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException | AccessDeniedException e) {
                // ignore, the file is not there no more; on Windows, if one thread concurrently deletes a file while
                // calling Files.size, you can also sometimes hit AccessDeniedException
            }
        }
        return estimatedSize;
    }

    private final Supplier<Long> size;

    ByteSizeCachingDirectory(Directory in, TimeValue refreshInterval) {
        super(in);
        size = Suppliers.memoizeWithExpiration(
            () -> {
                final long size;
                try {
                    // Compute this OUTSIDE of the lock
                    size = estimateSizeInBytes(getDelegate());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return size;
            },
            refreshInterval.duration(),
            refreshInterval.timeUnit()
        );
    }

    /** Return the cumulative size of all files in this directory. */
    Long estimateSizeInBytes() throws IOException {
        try {
            return size.get();
        } catch (UncheckedIOException e) {
            // we wrapped in the cache and unwrap here
            throw e.getCause();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return wrapIndexOutput(super.createOutput(name, context));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return wrapIndexOutput(super.createTempOutput(prefix, suffix, context));
    }

    private IndexOutput wrapIndexOutput(IndexOutput out) {
        return new FilterIndexOutput(out.toString(), out) {
            @Override
            public void writeBytes(byte[] b, int length) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeBytes(b, length);
            }

            @Override
            public void writeByte(byte b) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeByte(b);
            }

            @Override
            public void close() throws IOException {
                // Close might cause some data to be flushed from in-memory buffers, so
                // increment the modification counter too.
                super.close();
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
    }
}
