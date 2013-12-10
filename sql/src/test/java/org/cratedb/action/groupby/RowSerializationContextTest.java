package org.cratedb.action.groupby;

import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.count.CountDistinctAggFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class RowSerializationContextTest {

    @Test
    public void testIdxMapGeneration() {
        List<AggExpr> aggExprList = new ArrayList<>();
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col3", DataType.DOUBLE), true));
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));

        List<Integer> expected = new ArrayList<Integer>() {{
            add(0);
            add(1);
            add(0);
        }};

        RowSerializationContext context = new RowSerializationContext(aggExprList);
        assertEquals(expected, context.seenIdxMapping);
    }
}
