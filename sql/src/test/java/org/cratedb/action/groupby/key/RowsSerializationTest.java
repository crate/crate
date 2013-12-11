package org.cratedb.action.groupby.key;


import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.SQLGroupByResult;
import org.cratedb.action.SQLMapperResultRequest;
import org.cratedb.action.SQLReduceJobStatus;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.core.concurrent.FutureConcurrentMap;
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
import java.util.UUID;
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

        CacheRecycler cacheRecycler = new CacheRecycler(ImmutableSettings.EMPTY);
        NodeExecutionContext nec = HitchhikerMocks.nodeExecutionContext();
        TableExecutionContext tec = mock(TableExecutionContext.class);

        when(nec.tableContext(anyString(), anyString())).thenReturn(tec);

        when(tec.getCollectorExpression(any(ValueNode.class))).thenReturn(fakeExpression);

        SQLParseService parseService = new SQLParseService(nec);
        ParsedStatement stmt = parseService.parse(
                "select count(*), race from characters group by race");

        SQLMapperResultRequest requestSender = new SQLMapperResultRequest();
        GroupTree t1 = new GroupTree(1, stmt, cacheRecycler);
        requestSender.contextId = UUID.randomUUID();
        requestSender.groupByResult = new SQLGroupByResult(0, t1);

        t1.getRow();
        t1.getRow();

        BytesStreamOutput out = new BytesStreamOutput();
        requestSender.writeTo(out);


        FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs = FutureConcurrentMap.newMap();
        reduceJobs.put(requestSender.contextId, new SQLReduceJobStatus(stmt, 1));
        SQLMapperResultRequest requestReceiver = new SQLMapperResultRequest(
            reduceJobs, cacheRecycler
        );

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        requestReceiver.readFrom(in);

        GroupTree t2 = (GroupTree)requestReceiver.groupByResult.rows();

        assertEquals(t2.maps().length, t2.maps().length);
        assertMapEquals(t1.maps()[0], t2.maps()[0]);

    }
}