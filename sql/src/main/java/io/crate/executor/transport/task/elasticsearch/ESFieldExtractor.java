/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import io.crate.metadata.PartitionName;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Reference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ESFieldExtractor implements FieldExtractor<SearchHit> {

    private static final Object NOT_FOUND = new Object();

    public abstract Object extract(SearchHit hit);

    public static class Source extends ESFieldExtractor {

        private final ColumnIdent ident;

        public Source(ColumnIdent ident) {
            this.ident = ident;
        }

        @Override
        public Object extract(SearchHit hit) {
            return toValue(hit.getSource());
        }

        Object down(Object c, int idx) {
            if (idx == ident.path().size()) {
                return c == NOT_FOUND ? null : c;
            }
            if (c instanceof List) {
                List l = (List) c;
                ArrayList children = new ArrayList(l.size());
                for (Object child : l) {
                    Object sub = down(child, idx);
                    if (sub != NOT_FOUND) {
                        children.add(sub);
                    }
                }
                return children;
            } else if (c instanceof Map) {
                Map cm = ((Map) c);
                if (cm.containsKey(ident.path().get(idx))) {
                    return down(((Map) c).get(ident.path().get(idx)), idx + 1);
                } else {
                    return NOT_FOUND;
                }
            }
            throw new IndexOutOfBoundsException("Failed to get path");
        }

        /**
         *
         * This method extracts data for the given column ident, by traversing the source map.
         * If more than one object matches, the result is a list of the matching values, otherwise a single object.
         *
         * @param source the source as a map
         * @return the value calculated for the columnIdent
         */
        Object toValue(@Nullable Map<String, Object> source) {
            if (source == null || source.size() == 0) {
                return null;
            }
            Object top = source.get(ident.name());
            if (ident.isColumn()) {
                return top;
            }
            if (top==null){
                return null;
            }
            Object result = down(top, 0);
            return result == NOT_FOUND ? null : result;
        }
    }

    public static class PartitionedByColumnExtractor extends ESFieldExtractor {

        private final Reference reference;
        private final int valueIdx;
        private final Map<String, List<BytesRef>> cache;

        public PartitionedByColumnExtractor(Reference reference, List<ReferenceInfo> partitionedByInfos) {
            this.reference = reference;
            this.valueIdx = partitionedByInfos.indexOf(reference.info());
            this.cache = new HashMap<>();
        }

        @Override
        public Object extract(SearchHit hit) {
            try {
                List<BytesRef> values = cache.get(hit.index());
                if (values == null) {
                    values = PartitionName.fromStringSafe(hit.index()).values();
                }
                BytesRef value = values.get(valueIdx);
                if (value == null) {
                    return null;
                }
                return reference.info().type().value(value);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

}
