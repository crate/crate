package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

public class DoubleColumnReference extends FieldCacheExpression<IndexNumericFieldData, Double> {

    private DoubleValues values;

    public DoubleColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Double evaluate() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return values.nextValue();
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = (indexFieldData).load(context).getDoubleValues();
    }

    @Override
    public DataType returnType(){
        return DataType.DOUBLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof DoubleColumnReference))
            return false;
        return columnName.equals(((DoubleColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

