package org.cratedb.action.groupby.key;


import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.parser.parser.ValueNode;
import org.cratedb.stubs.HitchhikerMocks;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowsSerializationTest {

    static Expression fakeExpression = new Expression<BytesRef>(){

        AtomicInteger called = new AtomicInteger(0);

        @Override
        public BytesRef evaluate() {
            return new BytesRef(String.format("Fake call %03d", called.incrementAndGet()));
        }

        @Override
        public DataType returnType() {
            return DataType.STRING;
        }
    };

    private void assertMapEquals(Map m1, Map m2){
        String expected = new JSONObject(m1).toString();
        String actual = new JSONObject(m2).toString();
        assertEquals(expected, actual);
    }


    @Test
    public void testGroupTree() throws Exception {

        NodeExecutionContext nec = HitchhikerMocks.nodeExecutionContext();
        TableExecutionContext tec = mock(TableExecutionContext.class);

        when(nec.tableContext(anyString(), anyString())).thenReturn(tec);

        when(tec.getCollectorExpression(any(ValueNode.class))).thenReturn(fakeExpression);

        SQLParseService parseService = new SQLParseService(nec);
        ParsedStatement stmt = parseService.parse(
                "select name from characters group by name");
        CacheRecycler cacheRecycler = new CacheRecycler(ImmutableSettings.EMPTY);
        GroupTree t1 = new GroupTree(1, stmt, cacheRecycler);
        GroupTree t2 = new GroupTree(1, stmt, cacheRecycler);

        GroupByRow row = t1.getRow();
        row = t1.getRow();

        BytesStreamOutput out = new BytesStreamOutput();
        t1.writeBucket(out, 0);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        t2.readBucket(in, 0);

        assertEquals(t2.maps().length, t2.maps().length);
        assertMapEquals(t1.maps()[0], t2.maps()[0]);

    }
}