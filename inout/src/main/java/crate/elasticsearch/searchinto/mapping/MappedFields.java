package crate.elasticsearch.searchinto.mapping;

import crate.elasticsearch.action.searchinto.SearchIntoContext;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MappedFields {

    private final SearchIntoContext context;
    private SearchHit hit;
    private final List<OutputMapping> outputMappings;

    public MappedFields(SearchIntoContext context) {
        this.context = context;
        this.outputMappings = getOutputMappings();
    }

    public void hit(SearchHit hit) {
        this.hit = hit;
    }

    private List<OutputMapping> getOutputMappings() {
        List<OutputMapping> oms = new ArrayList<OutputMapping>(
                context.outputNames().size());
        boolean indexDefined = false;
        boolean typeDefined = false;
        for (Map.Entry<String, String> e : context.outputNames().entrySet()) {
            String srcName = e.getKey();
            String trgName = e.getValue();
            assert (trgName != null);
            if (trgName.equals("_index")) {
                indexDefined = true;
            } else if (trgName.equals("_type")) {
                typeDefined = true;
            }
            OutputMapping om = new OutputMapping(srcName, trgName);
            oms.add(om);
        }
        if (!indexDefined) {
            oms.add(new OutputMapping("_index", "_index"));
        }
        if (!typeDefined) {
            oms.add(new OutputMapping("_type", "_type"));
        }

        return oms;
    }

    public IndexRequest newIndexRequest() {
        IndexRequestBuilder builder = new IndexRequestBuilder();
        for (OutputMapping om : outputMappings) {
            om.setHit(hit);
            builder = om.toRequestBuilder(builder);
        }
        return builder.build();

    }
}

