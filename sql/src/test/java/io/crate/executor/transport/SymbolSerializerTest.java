package io.crate.executor.transport;

import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SymbolSerializerTest {


    @Test
    public void testValueSymbol() throws Exception {

        Value v = new Value(DataType.STRING);

        BytesStreamOutput out = new BytesStreamOutput();
        Symbol.toStream(v, out);


        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Value v2 = (Value) Symbol.fromStream(in);
        assertEquals(v2.valueType(), DataType.STRING);

    }
}
