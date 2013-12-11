package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

public class BytesRefColumnReference extends FieldCacheExpression<IndexFieldData, BytesRef> {

    private BytesValues values;

    public BytesRefColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public BytesRef evaluate() {
        if (values.setDocument(docId) == 0) {
            return null;
        }

        values.nextValue();
        return values.copyShared();
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getBytesValues(true);
    }

    @Override
    public DataType returnType() {
        return DataType.STRING;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof BytesRefColumnReference))
            return false;
        return columnName.equals(((BytesRefColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

