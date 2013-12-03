package org.cratedb.module.sql.test;

import org.cratedb.DataType;
import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.count.CountDistinctAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountStarAggFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class GroupByHelperTest {

    @Test
    public void testCreateIdxMappingCountDistinctManySame() {
        List<AggExpr> aggExprList = new ArrayList<>();
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));

        List<Integer> expected = new ArrayList<Integer>() {{
            add(0);
            add(0);
        }};
        List<Integer> idxMap = GroupByHelper.getSeenIdxMap(aggExprList);
        assertEquals(expected, idxMap);
    }

    @Test
    public void testCreateIdxMappingCountDistinctManySame2() {
        List<AggExpr> aggExprList = new ArrayList<>();
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));
        aggExprList.add(new AggExpr(CountStarAggFunction.NAME, null, false));
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));

        List<Integer> expected = new ArrayList<Integer>() {{
            add(0);
            add(0);
        }};
        List<Integer> idxMap = GroupByHelper.getSeenIdxMap(aggExprList);
        assertEquals(expected, idxMap);
    }

    @Test
    public void testCreateIdxMappingCountDistinctManyDifferent() {
        List<AggExpr> aggExprList = new ArrayList<>();
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col1", DataType.DOUBLE), true));
        aggExprList.add(new AggExpr(CountDistinctAggFunction.NAME, new ParameterInfo("col2", DataType.DOUBLE), true));

        List<Integer> expected = new ArrayList<Integer>() {{
            add(0);
            add(1);
        }};
        List<Integer> idxMap = GroupByHelper.getSeenIdxMap(aggExprList);
        assertEquals(expected, idxMap);
    }
}
