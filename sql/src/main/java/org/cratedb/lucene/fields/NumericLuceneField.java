package org.cratedb.lucene.fields;

import org.apache.lucene.index.IndexableField;

public abstract class NumericLuceneField<T extends Number> extends LuceneField<T> {

    public NumericLuceneField(String name, boolean allowMultipleValues) {
        super(name, allowMultipleValues);
    }

    @Override
    public Object getValue(IndexableField field) {
        return field.numericValue();
    }
}
