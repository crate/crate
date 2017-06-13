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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class DocKeys implements Iterable<DocKeys.DocKey> {

    private final int width;
    private final Function<List<BytesRef>, String> idFunction;
    private int clusteredByIdx;
    private final boolean withVersions;
    private final List<List<Symbol>> docKeys;
    private final List<Integer> partitionIdx;

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
            partitionIdx.add(in.readInt());
        }

        return new DocKeys(docKeys, withVersions, clusteredByIdx, partitionIdx);
    }

    public void toStream(StreamOutput out) throws IOException {
        int numDocKeys = 0;
        if (docKeys != null) {
            numDocKeys = docKeys.size();
        }
        out.writeVInt(numDocKeys);
        for (List<Symbol> d : docKeys) {
            Symbols.toStream(d, out);
        }

        out.writeBoolean(withVersions);

        out.writeInt(clusteredByIdx);

        int numPartitionIdx = 0;
        if (partitionIdx != null) {
            numPartitionIdx = partitionIdx.size();
        }
        out.writeVInt(numPartitionIdx);
        if (partitionIdx != null) {
            for (Integer i : partitionIdx) {
                out.writeInt(i);
            }
        }
    }

    public class DocKey {

        private List<Symbol> key;
        private String id;

        private DocKey(int pos) {
            key = docKeys.get(pos);
        }

        public Optional<Long> version() {
            if (withVersions && key.get(width) != null) {
                return Optional.of((Long) ((Literal) key.get(width)).value());
            }
            return Optional.empty();
        }

        public String routing() {
            if (clusteredByIdx >= 0) {
                return ValueSymbolVisitor.STRING.process(key.get(clusteredByIdx));
            } else {
                return id();
            }
        }

        public Optional<List<BytesRef>> partitionValues() {
            if (partitionIdx == null || partitionIdx.isEmpty()) {
                return Optional.empty();
            }
            List<BytesRef> values = Lists.transform(
                partitionIdx, pIdx -> ValueSymbolVisitor.BYTES_REF.process(key.get(pIdx)));
            return Optional.of(values);
        }

        public String id() {
            if (id == null) {
                id = idFunction.apply(pkValues(key));
            }
            return id;
        }

        private List<BytesRef> pkValues(List<Symbol> key) {
            return Lists.transform(key.subList(0, width), ValueSymbolVisitor.BYTES_REF.function);
        }

        public List<Symbol> values() {
            return key;
        }
    }

    public DocKeys(List<List<Symbol>> docKeys,
                   boolean withVersions,
                   int clusteredByIdx,
                   @Nullable List<Integer> partitionIdx) {
        this.partitionIdx = partitionIdx;
        assert docKeys != null && !docKeys.isEmpty() : "docKeys must not be null nor empty";
        if (withVersions) {
            this.width = docKeys.get(0).size() - 1;
        } else {
            this.width = docKeys.get(0).size();
        }
        this.withVersions = withVersions;
        this.docKeys = docKeys;
        this.clusteredByIdx = clusteredByIdx;
        this.idFunction = Id.compile(width, clusteredByIdx);
    }

    public boolean withVersions() {
        return withVersions;
    }

    public DocKey getOnlyKey() {
        Preconditions.checkState(size() == 1);
        return new DocKey(0);
    }

    public int size() {
        return docKeys.size();
    }

    @Override
    public Iterator<DocKey> iterator() {
        return new Iterator<DocKey>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < docKeys.size();
            }

            @Override
            public DocKey next() {
                return new DocKey(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported for " +
                                                        DocKeys.class.getSimpleName() + "$iterator");
            }
        };
    }
}
