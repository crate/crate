package org.cratedb.information_schema;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.cratedb.action.GroupByFieldLookup;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InformationSchemaFieldLookup implements GroupByFieldLookup {

    private final Map<String, InformationSchemaColumn> fieldMapper;
    int docId;
    private AtomicReader reader;

    public InformationSchemaFieldLookup(Map<String, InformationSchemaColumn> fieldMapper) {
        this.fieldMapper = fieldMapper;
    }

    @Override
    public void setNextDocId(int doc) {
        this.docId = doc;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.reader = context.reader();
    }

    @Override
    public Object lookupField(final String columnName)
        throws IOException, GroupByOnArrayUnsupportedException
    {
        Set<String> fieldsToLoad = new HashSet<>();
        fieldsToLoad.add(columnName);

        IndexableField[] fields = reader.document(docId, fieldsToLoad).getFields(columnName);
        if (fields.length > 1) {
            throw new GroupByOnArrayUnsupportedException(columnName);
        }

        return fieldMapper.get(columnName).getValue(fields[0]);
    }
}
