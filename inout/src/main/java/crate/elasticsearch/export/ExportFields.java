package crate.elasticsearch.export;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExportFields implements ToXContent {

    private final List<String> fields;
    private InternalSearchHit hit;
    private final List<FieldExtractor> fieldExtractors;

    static final class Fields {
        static final XContentBuilderString _SOURCE = new XContentBuilderString("_source");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString _TIMESTAMP = new XContentBuilderString(TimestampFieldMapper.NAME);
        static final XContentBuilderString _TTL = new XContentBuilderString(TTLFieldMapper.NAME);
    }

    abstract class FieldExtractor implements ToXContent {


    }


    class SourceFieldExtractor extends FieldExtractor {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            BytesReference source = hit.sourceRef();
            XContentType contentType = XContentFactory.xContentType(source);
            XContentParser parser = XContentFactory.xContent(contentType).createParser(source);
            try {
                parser.nextToken();
                builder.field(Fields._SOURCE);
                builder.copyCurrentStructure(parser);
            } finally {
                parser.close();
            }
            return builder;
        }
    }

    class HitFieldExtractor extends FieldExtractor {

        private final String fieldName;

        public HitFieldExtractor(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            SearchHitField field = hit.getFields().get(fieldName);
            if (field != null && !field.values().isEmpty()) {
                if (field.values().size() == 1) {
                    builder.field(field.name(), field.values().get(0));
                } else {
                    builder.field(field.name());
                    builder.startArray();
                    for (Object value : field.values()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
            return builder;
        }
    }

    public void hit(InternalSearchHit hit) {
        this.hit = hit;
    }

    public ExportFields(List<String> fields) {
        this.fields = fields;
        this.fieldExtractors = getFieldExtractors();
    }

    private List<FieldExtractor> getFieldExtractors() {
        List<FieldExtractor> extractors = new ArrayList<FieldExtractor>(fields.size());
        for (String fn : fields) {
            FieldExtractor fc = null;
            if (fn.startsWith("_")) {
                if (fn.equals("_source")) {
                    fc = new SourceFieldExtractor();
                } else if (fn.equals("_id")) {
                    fc = new FieldExtractor() {
                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return builder.field(Fields._ID, hit.getId());
                        }
                    };
                } else if (fn.equals("_version")) {
                    fc = new FieldExtractor() {
                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return builder.field(Fields._VERSION, hit.getVersion());
                        }
                    };
                } else if (fn.equals("_index")) {
                    fc = new FieldExtractor() {
                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return builder.field(Fields._INDEX, hit.getIndex());
                        }
                    };
                } else if (fn.equals("_type")) {
                    fc = new FieldExtractor() {
                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return builder.field(Fields._TYPE, hit.getType());
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (FieldExtractor fc : fieldExtractors) {
            fc.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}

