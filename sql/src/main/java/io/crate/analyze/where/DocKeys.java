/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.where;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.crate.analyze.Id;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Holds the values of the primary key. In case of a non-compound
 * primary key, it is a list of possible values to look for during
 * query execution. In case of a compound key, multiple values are
 * provided for each possible match.
 *
 * Further, this class provides an "id" function to convert the values
 * into a String for performing a lookup with the Elasticsearch API.
 *
 * In addition, this class holds information about the routing index
 * in the list of values.
 *
 * Finally, the partitioning can be performed on one or multiple values
 * in the list. The relevant indices are held in this class.
 *
 * DocKey examples:
 *      select * from table where id=1
 *          => docKeys: [[1]]
 *      select * from table where id=1 or id=2
 *          => docKeys: [[1], [2]]
 *      select * from table2 where id = 1 and (id2 = 0 or id2 = 2)
 *          => docKeys [[1, 0], [1, 2]]
 *
 *   Version, if supplied is the last value of the key list:
 *
 *      select * from table where _version=3 and (id=1 or id=2)
 *          => docKeys: [[1, 3], [2, 3]]
 *
 *   The version is part of the values because it is also a Symbol
 *   that is can only replaced at runtime.
 *
 */
public class DocKeys implements Iterable<DocKeys.DocKey>, Writeable {

    private int clusteredByIdx;
    private final boolean withVersions;
    private final List<List<Symbol>> docKeys;
    private final List<Integer> partitionIdx;

    public DocKeys(List<List<Symbol>> docKeys,
                   boolean withVersions,
                   int clusteredByIdx,
                   @Nullable List<Integer> partitionIdx) {
        assert docKeys != null && !docKeys.isEmpty() : "docKeys must not be null nor empty";
        final int width;
        if (withVersions) {
            width = docKeys.get(0).size() - 1;
        } else {
            width = docKeys.get(0).size();
        }
        assert clusteredByIdx < width;
        this.clusteredByIdx = clusteredByIdx;
        this.partitionIdx = partitionIdx != null ? partitionIdx : Collections.emptyList();
        this.docKeys = docKeys;
        this.withVersions = withVersions;
    }


    public static DocKeys fromStream(StreamInput in) throws IOException {
        int numDocKeys = in.readVInt();
        List<List<Symbol>> docKeys = new ArrayList<>(numDocKeys);
        for (int i = 0; i < numDocKeys; i++) {
            docKeys.add(Symbols.listFromStream(in));
        }

        boolean withVersions = in.readBoolean();

        int clusteredByIdx = in.readInt();

        int numPartitionIdx = in.readVInt();
        List<Integer> partitionIdx = new ArrayList<>(numPartitionIdx);
        for (int i = 0; i < numPartitionIdx; i++) {
            partitionIdx.add(in.readVInt());
        }

        return new DocKeys(docKeys, withVersions, clusteredByIdx, partitionIdx);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        int numDocKeys = docKeys.size();
        out.writeVInt(numDocKeys);
        for (int i = 0; i < numDocKeys; i++) {
            Symbols.toStream(docKeys.get(i), out);
        }

        out.writeBoolean(withVersions);

        out.writeInt(clusteredByIdx);

        out.writeVInt(partitionIdx.size());
        for (Integer i : partitionIdx) {
            out.writeVInt(i);
        }
    }

    public static class DocKey implements Writeable {

        private final List<Symbol> key;
        private final int clusteredByIdx;
        private final List<Integer> partitionedByIdx;
        private final boolean withVersion;
        private transient String id;

        private DocKey(List<Symbol> key, int clusteredByIdx, List<Integer> partitionedByIdx, boolean withVersion) {
            this.key = key;
            this.clusteredByIdx = clusteredByIdx;
            this.partitionedByIdx = partitionedByIdx != null ? partitionedByIdx : Collections.emptyList();
            this.withVersion = withVersion;
        }

        public Optional<Long> version() {
            if (!withVersion) {
                return Optional.empty();
            }
            Symbol versionSymbol = key.get(key.size() - 1);
            if (versionSymbol instanceof Literal) {
                Literal versionLiteral = (Literal) versionSymbol;
                if (versionLiteral.value() instanceof Long) {
                    return Optional.of((Long) versionLiteral.value());
                }
            }
            throw new RuntimeException("Version supplied in DocKey is invalid");
        }

        public String routing() {
            if (clusteredByIdx >= 0) {
                return ValueSymbolVisitor.STRING.process(key.get(clusteredByIdx));
            } else {
                return id();
            }
        }

        public Optional<List<BytesRef>> partitionValues() {
            if (partitionedByIdx == null || partitionedByIdx.isEmpty()) {
                return Optional.empty();
            }
            List<BytesRef> values = Lists.transform(
                partitionedByIdx, pIdx -> ValueSymbolVisitor.BYTES_REF.process(key.get(pIdx)));
            return Optional.of(values);
        }

        public String id() {
            if (id == null) {
                int width = withVersion ? key.size() - 1 : key.size();
                id = Id
                    .compile(width, clusteredByIdx)
                    .apply(Lists.transform(key.subList(0, width), ValueSymbolVisitor.BYTES_REF.function));
            }
            return id;
        }

        public List<Symbol> values() {
            return key;
        }

        public static DocKey readFrom(StreamInput in) throws IOException {
            List<Symbol> key = Symbols.listFromStream(in);

            int clusterIndex = in.readVInt();

            int partitionIndexLen = in.readVInt();
            List<Integer> partitionIndexList = new ArrayList<>(partitionIndexLen);
            for (int i = 0; i < partitionIndexLen; i++) {
                partitionIndexList.add(in.readVInt());
            }

            boolean withVersion = in.readBoolean();

            return new DocKey(key, clusterIndex, partitionIndexList, withVersion);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Symbols.toStream(key, out);

            out.writeVInt(clusteredByIdx);

            out.writeVInt(partitionedByIdx.size());
            for (Integer index : partitionedByIdx) {
                out.writeVInt(index);
            }

            out.writeBoolean(withVersion);
        }
    }


    private DocKey createDocKey(List<Symbol> docKey) {
        return new DocKey(docKey, clusteredByIdx, partitionIdx, withVersions);
    }

    public boolean withVersions() {
        return withVersions;
    }

    public DocKey getOnlyKey() {
        Preconditions.checkState(docKeys.size() == 1);
        return createDocKey(docKeys.get(0));
    }

    public int size() {
        return docKeys.size();
    }

    @Override
    public Iterator<DocKey> iterator() {
        return new Iterator<DocKey>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < docKeys.size();
            }

            @Override
            public DocKey next() {
                return createDocKey(docKeys.get(i++));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported for " +
                    DocKeys.class.getSimpleName() + "$iterator");
            }
        };
    }
}
