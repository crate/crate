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
import io.crate.analyze.symbol.ParamSymbols;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.data.Row;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.Collections;
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

    public class DocKey {

        private final List<Symbol> key;
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

        /**
         * @deprecated use {@link #getId(Row)} instead
         */
        @Deprecated
        public String id() {
            if (id == null) {
                id = idFunction.apply(pkValues(key));
            }
            return id;
        }

        public String getId(Row params) {
            return idFunction.apply(
                Lists.transform(key.subList(0, width),
                    s -> ValueSymbolVisitor.BYTES_REF.function.apply(ParamSymbols.toLiterals(s, params))));
        }

        private List<BytesRef> pkValues(List<Symbol> key) {
            return Lists.transform(key.subList(0, width), ValueSymbolVisitor.BYTES_REF.function);
        }

        public List<Symbol> values() {
            return key;
        }

        public List<BytesRef> getPartitionValues(Row params) {
            if (partitionIdx == null || partitionIdx.isEmpty()) {
                return Collections.emptyList();
            }
            return Lists.transform(
                partitionIdx,
                pIdx -> ValueSymbolVisitor.BYTES_REF.function.apply(ParamSymbols.toLiterals(key.get(pIdx), params)));

        }

        public String getRouting(Row params) {
            if (clusteredByIdx >= 0) {
                return ValueSymbolVisitor.STRING.process(ParamSymbols.toLiterals(key.get(clusteredByIdx), params));
            }
            return getId(params);
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
