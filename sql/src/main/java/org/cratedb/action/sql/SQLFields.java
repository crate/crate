package org.cratedb.action.sql;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLFields {

    private final List<Tuple<String, String>> fields;
    private SearchHit hit;
    private final List<FieldExtractor> fieldExtractors;

    public SQLFields(List<Tuple<String, String>> outputFields) {
        this.fields = outputFields;
        fieldExtractors = getFieldExtractors();
    }

    public Object[] getRowValues() {
        Object [] values = new Object[this.fields.size()];
        for (int i = 0; i < fieldExtractors.size(); i++) {
            values[i] = fieldExtractors.get(i).getValue();
        }
        return values;
    }

    abstract class FieldExtractor {
        public abstract Object getValue();
    }

    class SourceFieldExtractor extends FieldExtractor {

        @Override
        public Map<String, Object> getValue() {
            BytesReference s = hit.sourceRef();
            if (s!=null){
                return XContentHelper.convertToMap(s,false).v2();
            }
            return null;
        }

    }

    class HitFieldExtractor extends FieldExtractor {

        private final String fieldName;

        public HitFieldExtractor(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Object getValue() {
            SearchHitField field = hit.getFields().get(fieldName);
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

    public void hit(SearchHit hit) {
        this.hit = hit;
    }

    public void applyGetResponse(ITableExecutionContext tableContext, GetResponse getResponse) {
        this.hit = searchHitFromGetResponse(tableContext, getResponse);
    }

    private List<FieldExtractor> getFieldExtractors() {
        List<FieldExtractor> extractors = new ArrayList<>(fields.size());
        for (Tuple<String, String> t : fields) {
            String fn = t.v2();
            FieldExtractor fc = null;
            if (fn.startsWith("_")) {
                if (fn.equals("_source")) {
                    fc = new SourceFieldExtractor();
                } else if (fn.equals("_id")) {
                    fc = new FieldExtractor() {
                        @Override
                        public Object getValue() {
                            return hit.getId();
                        }
                    };
                } else if (fn.equals("_version")) {
                    fc = new FieldExtractor() {
                        @Override
                        public Object getValue() {
                            return hit.getVersion();
                        }
                    };
                } else if (fn.equals("_score")) {
                    fc = new FieldExtractor() {
                        @Override
                        public Object getValue() {
                            return hit.getScore();
                        }
                    };
                } else {
                    fc = new HitFieldExtractor(fn);
                }
            } else {
                fc = new HitFieldExtractor(fn);
            }
            extractors.add(fc);
        }
        return extractors;
    }

    /**
     * build a SearchHit out of a GetResponse
     * Apply DocumentMapping again as Realtime-GetRequests only fetch from source
     */
    private SearchHit searchHitFromGetResponse(ITableExecutionContext tableContext, GetResponse getResponse) {

        Map<String, SearchHitField> searchFields = new HashMap<>(getResponse.getFields().size());
        for (Map.Entry<String, GetField> entry : getResponse.getFields().entrySet()) {
            searchFields.put(entry.getKey(), new InternalSearchHitField(
                    entry.getKey(),
                    entry.getValue().getValues()
                )
            );
        }

        BytesReference source = null;
        if (getResponse.getSourceInternal() != null) {
            source = getResponse.getSourceAsBytesRef();
        }

        InternalSearchHit searchHit = new InternalSearchHit(0, getResponse.getId(),
                new StringAndBytesText(getResponse.getType()), source,
                searchFields);
        searchHit.version(getResponse.getVersion());

        return searchHit;
    }
}

