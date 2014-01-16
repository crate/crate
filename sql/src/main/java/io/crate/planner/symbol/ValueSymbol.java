package io.crate.planner.symbol;

import com.google.common.base.Function;
import org.cratedb.DataType;

import javax.annotation.Nullable;

public abstract class ValueSymbol extends Symbol {

    public abstract DataType valueType();

    public static Function<ValueSymbol, DataType> valueTypeGetter = new Function<ValueSymbol, DataType>() {
        @Nullable
        @Override
        public DataType apply(@Nullable ValueSymbol input) {
            return input.valueType();
        }
    };
}
