package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

public class LongColumnReference extends FieldCacheExpression<IndexNumericFieldData, Long> {

    private LongValues values;

    public LongColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Long evaluate() {
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
        values = indexFieldData.load(context).getLongValues();
    }

    @Override
    public DataType returnType(){
        return DataType.LONG;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof LongColumnReference))
            return false;
        return columnName.equals(((LongColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

