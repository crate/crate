package crate.elasticsearch.rest.action;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.io.IOException;
import java.util.Map;

public class SQLResponseBuilder {

    public void build(SearchResponse response, XContentBuilder builder,
                      Map<String, String> fieldNameMapping) throws IOException {

        builder.startArray("rows");
        SearchHit[] searchHits = response.getHits().getHits();

        Map<String, SearchHitField> fields;
        for (SearchHit hit : searchHits) {
            builder.startObject();
            fields = hit.getFields();

            for (Map.Entry<String, String> entry : fieldNameMapping.entrySet()) {

                if (entry.getKey().equals("*")) {
                    builder.field("_index", hit.getIndex());
                    builder.field("_type", hit.getType());
                    builder.field("_id", hit.getId());
                    builder.field("_source", hit.getSource());
                } else if (entry.getValue().equals("_id")) {
                    builder.field(entry.getKey(), hit.getId());
                } else if (entry.getValue().equals("_index")) {
                    builder.field(entry.getKey(), hit.getIndex());
                } else if (entry.getValue().equals("_type")) {
                    builder.field(entry.getKey(), hit.getType());
                } else if (entry.getValue().equals("_source")) {
                    builder.field(entry.getKey(), hit.getSource());
                } else if (entry.getValue().equals("_version")) {
                    builder.field(entry.getKey(), hit.getVersion());
                } else if (fields != null) {
                    try {
                        builder.field(entry.getKey(), fields.get(entry.getValue()).getValue());
                    } catch (NullPointerException ex) {
                        builder.field(entry.getKey(), (String)null);
                    }
                }
            }
            builder.endObject();
        }
        builder.endArray();
    }
}
