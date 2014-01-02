package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

public class BooleanColumnReference extends FieldCacheExpression<IndexFieldData, Boolean> {

    private BytesValues values;
    private static final BytesRef TRUE_BYTESREF = new BytesRef("T");

    public BooleanColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public DataType returnType(){
        return DataType.BOOLEAN;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getBytesValues(false);
    }

    @Override
    public Boolean evaluate() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return values.nextValue().compareTo(TRUE_BYTESREF) == 0;
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof BooleanColumnReference))
            return false;
        return columnName.equals(((BooleanColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}