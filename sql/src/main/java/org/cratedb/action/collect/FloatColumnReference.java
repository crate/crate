package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

public class FloatColumnReference extends FieldCacheExpression<IndexNumericFieldData, Float> {

    DoubleValues values;

    public FloatColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Float evaluate() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return ((Double)values.nextValue()).floatValue();
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getDoubleValues();
    }

    @Override
    public DataType returnType(){
        return DataType.FLOAT;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof FloatColumnReference))
            return false;
        return columnName.equals(((FloatColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}
