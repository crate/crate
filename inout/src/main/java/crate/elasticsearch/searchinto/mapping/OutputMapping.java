package crate.elasticsearch.searchinto.mapping;

import org.elasticsearch.search.SearchHit;

public class OutputMapping {

    private final String srcName;
    private final String trgName;
    private final String srcLiteral;
    private final FieldReader reader;
    private final FieldWriter writer;
    private SearchHit hit;


    private static String getLiteral(String candidate) {
        if (candidate != null && candidate.length() > 2) {
            if ((candidate.startsWith("'") && candidate.endsWith(
                    "'")) || (candidate.startsWith("\"") && candidate.endsWith(
                    "\""))) {
                return candidate.substring(1, candidate.length() - 1);
            }
        }
        return null;
    }

    public OutputMapping(String srcName, String trgName) {
        this.srcName = srcName;
        this.trgName = trgName;
        srcLiteral = getLiteral(srcName);
        if (srcLiteral == null) {
            this.reader = new FieldReader(srcName);
        } else {
            this.reader = null;
        }
        this.writer = new FieldWriter(trgName);
    }

    public void setHit(SearchHit hit) {
        this.hit = hit;
    }

    public IndexRequestBuilder toRequestBuilder(IndexRequestBuilder builder) {
        if (srcLiteral != null) {
            writer.setValue(srcLiteral);
        } else {
            reader.setHit(hit);
            writer.setValue(reader.getValue());

        }
        writer.toRequestBuilder(builder);
        return builder;
    }

}
