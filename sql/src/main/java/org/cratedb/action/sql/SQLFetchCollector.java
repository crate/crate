package org.cratedb.action.sql;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.FieldLookup;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLFetchCollector extends Collector {

    private final ParsedStatement parsedStatement;
    private final FieldLookup fieldLookup;

    public List<List<Object>> results = new ArrayList();

    public SQLFetchCollector(ParsedStatement parsedStatement, FieldLookup fieldLookup) {
        this.parsedStatement = parsedStatement;
        this.fieldLookup = fieldLookup;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        List<Object> rowResult = new ArrayList<>(parsedStatement.outputFields.size());
        for (Tuple<String, String> columnNames : parsedStatement.outputFields()) {
            rowResult.add(fieldLookup.lookupField(columnNames.v2()));
        }
        results.add(rowResult);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
