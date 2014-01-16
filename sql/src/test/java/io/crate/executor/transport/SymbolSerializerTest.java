package io.crate.executor.transport;

import io.crate.planner.symbol.Routing;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SymbolSerializerTest {


    @Test
    public void testRoutingSymbol() throws Exception {
        Map<String, Map<String, Integer>> locations = new HashMap<>();
        Map<String, Integer> innerLocation = new HashMap<>();
        innerLocation.put("dummyIndex", 0);
        locations.put("node1", innerLocation);
        Routing routing1 = new Routing(locations);

        BytesStreamOutput out = new BytesStreamOutput();
        Symbol.toStream(routing1, out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Routing routing2 = (Routing)Symbol.fromStream(in);


        assertEquals(routing1.locations(), routing2.locations());
    }
}
