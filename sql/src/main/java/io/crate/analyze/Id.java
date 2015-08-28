/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

public class Id {

    private final static Function<List<BytesRef>, String> RANDOM_ID = new Function<List<BytesRef>, String>() {
        @Nullable
        @Override
        public String apply(@Nullable List<BytesRef> input) {
            return Strings.base64UUID();
        }
    };

    private final static Function<List<BytesRef>, String> ONLY_ITEM = new Function<List<BytesRef>, String>() {
        @Nullable
        @Override
        public String apply(@Nullable List<BytesRef> input) {
            assert input != null : "input must not be null";
            return ensureNonNull(getOnlyElement(input)).utf8ToString();
        }
    };

    /**
     * generates a function which can be used to generate an id.
     *
     * This variant doesn't handle the pk = _id case. Use {@link #compile(List, ColumnIdent)} if it should be handled
     */
    public static Function<List<BytesRef>, String> compile(final int numPks, final int clusteredByPosition) {
        switch (numPks) {
            case 0:
                return RANDOM_ID;
            case 1:
                return ONLY_ITEM;
            default:
                return new Function<List<BytesRef>, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable List<BytesRef> input) {
                        assert input != null : "input must not be null";
                        if (input.size() != numPks) {
                            throw new IllegalArgumentException("Missing primary key values");
                        }
                        return encode(input, clusteredByPosition);
                    }
                };
        }
    }

    /**
     * returns a function which can be used to generate an id
     */
    public static Function<List<BytesRef>, String> compile(final List<ColumnIdent> pkColumns, final ColumnIdent clusteredBy) {
        final int numPks = pkColumns.size();
        if (numPks == 1 && getOnlyElement(pkColumns).equals(DocSysColumns.ID)) {
            return RANDOM_ID;
        }
        return compile(numPks, pkColumns.indexOf(clusteredBy));
    }

    @Nonnull
    private static BytesRef ensureNonNull(@Nullable BytesRef pkValue) throws IllegalArgumentException {
        if (pkValue == null) {
            throw new IllegalArgumentException("A primary key value must not be NULL");
        }
        return pkValue;
    }

    private static String encode(List<BytesRef> values, int clusteredByPosition) {
        try (BytesStreamOutput out = new BytesStreamOutput(estimateSize(values))) {
            int size = values.size();
            out.writeVInt(size);
            if (clusteredByPosition >= 0) {
                out.writeBytesRef(ensureNonNull(values.get(clusteredByPosition)));
            }
            for (int i = 0; i < size; i++) {
                if (i != clusteredByPosition) {
                    out.writeBytesRef(ensureNonNull(values.get(i)));
                }
            }
            return Base64.encodeBytes(out.bytes().toBytes());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * estimates the size the bytesRef values will take if written onto a StreamOutput using the String streamer
     */
    private static int estimateSize(Iterable<BytesRef> values) {
        int expectedEncodedSize = 0;
        for (BytesRef value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length : 0);
        }
        return expectedEncodedSize;
    }
}
