package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.util.List;

public abstract class ScriptValuesColumnReference<ReturnType> extends
        FieldCacheExpression<IndexFieldData, ReturnType> {

    private ScriptDocValues values;

    public ScriptValuesColumnReference(String columnName) {
        super(columnName);
    }

    Object scriptEvaluate() {
        values.setNextDocId(docId);
        if (values.isEmpty()){
            return null;
        }
        return values.getValues().get(0);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getScriptValues();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ScriptValuesColumnReference))
            return false;
        return columnName.equals(((ScriptValuesColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

