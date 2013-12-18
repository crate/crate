package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

public class IntegerColumnReference extends FieldCacheExpression<IndexNumericFieldData, Integer> {

    private LongValues values;

    public IntegerColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Integer evaluate() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return ((Long)values.nextValue()).intValue();
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
        return DataType.INTEGER;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof IntegerColumnReference))
            return false;
        return columnName.equals(((IntegerColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

