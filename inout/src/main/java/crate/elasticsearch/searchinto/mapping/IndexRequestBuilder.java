package crate.elasticsearch.searchinto.mapping;

import org.elasticsearch.action.index.IndexRequest;

import java.util.HashMap;
import java.util.Map;


public class IndexRequestBuilder {

    Map<String, Object> source = new HashMap<String, Object>();
    final IndexRequest request = new IndexRequest();

    final Map<String, String> meta = new HashMap<String, String>();

    public IndexRequest build() {
        request.source(source);
        return request;
    }

}
