package crate.elasticsearch.searchinto.mapping;

import org.elasticsearch.common.collect.ImmutableMap;
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
