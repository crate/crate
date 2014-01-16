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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.searchinto.mapping;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.util.Map;

public class FieldReader {

    private final String name;
    private HitReader reader;
    private SearchHit hit;

    private final static ImmutableMap<String, HitReader> readers;

    static {
        readers = MapBuilder.<String, HitReader>newMapBuilder().put(
                SourceFieldMapper.NAME, new HitReader<Map<String, Object>>() {
            @Override
            public Map<String, Object> read(SearchHit hit) {
                return hit.getSource();
            }
        }).put(IndexFieldMapper.NAME, new HitReader<String>() {
            @Override
            public String read(SearchHit hit) {
                return hit.index();
            }
        }).put(TypeFieldMapper.NAME, new HitReader<String>() {
            @Override
            public String read(SearchHit hit) {
                return hit.type();
            }
        }).put(IdFieldMapper.NAME, new HitReader<String>() {
            @Override
            public String read(SearchHit hit) {
                return hit.id();
            }
        }).put(TimestampFieldMapper.NAME, new HitReader<Long>() {
            @Override
            public Long read(SearchHit hit) {
                SearchHitField field = hit.getFields().get(
                        TimestampFieldMapper.NAME);
                if (field != null && !field.values().isEmpty()) {
                    return field.value();
                }
                return null;
            }
        }).put(TTLFieldMapper.NAME, new HitReader<Long>() {
            @Override
            public Long read(SearchHit hit) {
                SearchHitField field = hit.getFields().get(
                        TTLFieldMapper.NAME);
                if (field != null && !field.values().isEmpty()) {
                    return field.value();
                }
                return null;
            }
        }).put("_version", new HitReader<Long>() {
            @Override
            public Long read(SearchHit hit) {
                return hit.getVersion();
            }
        }).immutableMap();
    }

    static abstract class HitReader<T> {
        public abstract T read(SearchHit hit);
    }

    static class HitFieldReader extends HitReader {

        private final String name;

        HitFieldReader(String name) {
            this.name = name;
        }

        @Override
        public Object read(SearchHit hit) {
            SearchHitField field = hit.getFields().get(name);
            if (field != null && !field.values().isEmpty()) {
                if (field.values().size() == 1) {
                    return field.values().get(0);
                } else {
                    return field.values();
                }
            }
            return null;
        }
    }

    public FieldReader(String name) {
        this.name = name;
        initReader();
    }


    private void initReader() {
        if (name.startsWith("_")) {
            reader = readers.get(name);
        }
        if (reader == null) {
            reader = new HitFieldReader(name);
        }
    }

    public void setHit(SearchHit hit) {
        this.hit = hit;
    }

    public Object getValue() {
        return reader.read(hit);
    }

}
