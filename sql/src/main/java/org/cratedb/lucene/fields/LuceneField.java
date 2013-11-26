package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.cratedb.index.ColumnDefinition;

import java.io.IOException;

public abstract class LuceneField<E extends Object> {

    public final String name;
    public final boolean allowMultipleValues;
    public SortField.Type type;
    public Analyzer analyzer = new KeywordAnalyzer();

    public LuceneField(String name, boolean allowMultipleValues) {
        this.name = name;
        this.allowMultipleValues = allowMultipleValues;
    }

    public abstract Object getValue(IndexableField field);
    public abstract MultiTermQuery rangeQuery(Object from, Object to,
                                              boolean includeLower, boolean includeUpper);
    public abstract MultiTermQueryWrapperFilter rangeFilter(
        Object from, Object to, boolean includeLower, boolean includeUpper);

    public abstract ColumnDefinition getColumnDefinition(String tableName, int ordinalPosition);

    public abstract Object mappedValue(Object value);

    public abstract Field field(E value);

    public abstract TokenStream tokenStream(E value) throws IOException;
}
