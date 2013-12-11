package org.cratedb.action.collect;

import org.cratedb.DataType;

import java.util.List;

public class BooleanColumnReference extends ScriptValuesColumnReference<Boolean> {

    public BooleanColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public DataType returnType(){
        return DataType.BOOLEAN;
    }

    @Override
    public Boolean evaluate() {
        Object v = scriptEvaluate();
        if (v!=null){
            return (Boolean) v;
        }
        return null;
    }
}
