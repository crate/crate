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

package io.crate.analyze;

import static io.crate.common.collections.Lists.getOnlyElement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;


public class Id {

    private static final Function<List<String>, String> RANDOM_ID = ignored -> UUIDs.base64UUID();

    private static final Function<List<String>, String> ONLY_ITEM_NULL_VALIDATION = keyValues -> {
        return ensureNonNull(getOnlyElement(keyValues));
    };

    private static final Function<List<String>, String> ONLY_ITEM = Lists::getOnlyElement;


    /**
     * generates a function which can be used to generate an id and apply null validation.
     * <p>
     * This variant doesn't handle the pk = _id case.
     */
    private static Function<List<String>, String> compileWithNullValidation(final int numPks,
                                                                            final int clusteredByPosition) {
        switch (numPks) {
            case 0:
                return RANDOM_ID;
            case 1:
                return ONLY_ITEM_NULL_VALIDATION;
            default:
                return keyValues -> {
                    if (keyValues.size() != numPks) {
                        throw new IllegalArgumentException("Missing primary key values");
                    }
                    return encode(keyValues, clusteredByPosition);
                };
        }
    }

    /**
     * generates a function which can be used to generate an id.
     * <p>
     * This variant doesn't handle the pk = _id case.
     */
    public static Function<List<String>, String> compile(final int numPks, final int clusteredByPosition) {
        if (numPks == 1) {
            return ONLY_ITEM;
        }
        return compileWithNullValidation(numPks, clusteredByPosition);
    }

    /**
     * returns a function which can be used to generate an id with null validation.
     */
    public static Function<List<String>, String> compileWithNullValidation(final List<ColumnIdent> pkColumns, final ColumnIdent clusteredBy) {
        final int numPks = pkColumns.size();
        if (numPks == 1 && getOnlyElement(pkColumns).equals(DocSysColumns.ID.COLUMN)) {
            return RANDOM_ID;
        }
        int idx = -1;
        if (clusteredBy != null) {
            idx = pkColumns.indexOf(clusteredBy);
        }
        return compileWithNullValidation(numPks, idx);
    }

    @NotNull
    private static <T> T ensureNonNull(@Nullable T pkValue) throws IllegalArgumentException {
        if (pkValue == null) {
            throw new IllegalArgumentException("A primary key value must not be NULL");
        }
        return pkValue;
    }

    public static List<String> decode(List<ColumnIdent> primaryKeys, String id) {
        if (primaryKeys.isEmpty() || primaryKeys.size() == 1) {
            return List.of(id);
        }
        List<String> pkValues = new ArrayList<>();
        byte[] inputBytes = Base64.getDecoder().decode(id);
        try (var in = StreamInput.wrap(inputBytes)) {
            int size = in.readVInt();
            assert size == primaryKeys.size()
                : "Encoded primary key values must match size of primaryKey column list";
            for (int i = 0; i < size; i++) {
                BytesRef bytesRef = in.readBytesRef();
                pkValues.add(bytesRef.utf8ToString());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return pkValues;
    }

    public static String encode(List<String> values, int clusteredByPosition) {
        try (BytesStreamOutput out = new BytesStreamOutput(estimateSize(values))) {
            int size = values.size();
            out.writeVInt(size);
            if (clusteredByPosition >= 0) {
                out.writeBytesRef(new BytesRef(ensureNonNull(values.get(clusteredByPosition))));
            }
            for (int i = 0; i < size; i++) {
                if (i != clusteredByPosition) {
                    out.writeBytesRef(new BytesRef(ensureNonNull(values.get(i))));
                }
            }
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * estimates the size the bytesRef values will take if written onto a StreamOutput using the String streamer
     */
    private static int estimateSize(Iterable<String> values) {
        int expectedEncodedSize = 0;
        for (String value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length() : 0);
        }
        return expectedEncodedSize;
    }
}
