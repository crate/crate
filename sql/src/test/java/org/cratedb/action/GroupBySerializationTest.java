package org.cratedb.action;


import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.RowSerializationContext;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GroupBySerializationTest {


    @Test
    public void testMapperToReducerSerialization() throws Exception {
        SQLParseService parseService = new SQLParseService(HitchhikerMocks.nodeExecutionContext());
        ParsedStatement stmt = parseService.parse("select count(*), name from characters group by name");

        BytesStreamOutput out = new BytesStreamOutput();

        SQLMapperResultRequest requestSender = new SQLMapperResultRequest();
        requestSender.contextId = UUID.randomUUID();
        requestSender.groupByResult = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k1"}), new ArrayList<AggState>() {{
                add(new CountAggState() {{ value = 2; }});
            }}),
            new GroupByRow(new GroupByKey(new Object[] {"k2"}), new ArrayList<AggState>() {{
                add(new CountAggState() {{ value = 3; }});
            }})
        ));

        requestSender.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());

        SQLReduceJobStatus reduceJob = new SQLReduceJobStatus(stmt, 1);
        FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs = FutureConcurrentMap.newMap();
        reduceJobs.put(requestSender.contextId, reduceJob);
        SQLMapperResultRequest requestReceiver = new SQLMapperResultRequest(reduceJobs);
        requestReceiver.readFrom(in);

        assertThat(requestReceiver.groupByResult.result().size(), is(2));
    }

    @Test
    public void testMapperToReducerSerializationWithDistinct() throws Exception {
        SQLParseService parseService = new SQLParseService(HitchhikerMocks.nodeExecutionContext());
        ParsedStatement stmt = parseService.parse("select count(distinct gender), race from characters group by race");

        BytesStreamOutput out = new BytesStreamOutput();


        SQLMapperResultRequest requestSender = new SQLMapperResultRequest();
        requestSender.contextId = UUID.randomUUID();

        RowSerializationContext rowSerializationContext = new RowSerializationContext(stmt.aggregateExpressions);

        final Set<Object> k1seenValues = newHashSet((Object)"male");
        final Set<Object> k2seenValues = newHashSet((Object)"male", "female");
        final GroupByRow row1 = GroupByRow.createEmptyRow(new GroupByKey(new Object[] {"k1"}), rowSerializationContext);
        row1.aggStates = new ArrayList<AggState>() {{ add(new CountAggState() {{ seenValues = k1seenValues; }}); }};
        row1.seenValuesList = new ArrayList<>();
        row1.seenValuesList.add(k1seenValues);

        final GroupByRow row2 = GroupByRow.createEmptyRow(new GroupByKey(new Object[] {"k2"}), rowSerializationContext);
        row2.aggStates = new ArrayList<AggState>() {{ add(new CountAggState() {{ seenValues = k2seenValues; }}); }};
        row2.seenValuesList = new ArrayList<>();
        row2.seenValuesList.add(k2seenValues);

        requestSender.groupByResult = new SQLGroupByResult(Arrays.asList(row1, row2));
        requestSender.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());

        SQLReduceJobStatus reduceJob = new SQLReduceJobStatus(stmt, 1);
        FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs = FutureConcurrentMap.newMap();
        reduceJobs.put(requestSender.contextId, reduceJob);
        SQLMapperResultRequest requestReceiver = new SQLMapperResultRequest(reduceJobs);
        requestReceiver.readFrom(in);

        assertThat(requestReceiver.groupByResult.result().size(), is(2));

        ArrayList<GroupByRow> rows = new ArrayList<>(requestReceiver.groupByResult.result());
        rows.get(0).aggStates.get(0).terminatePartial();
        assertThat((Long)rows.get(0).aggStates.get(0).value(), is(1L));

        rows.get(1).aggStates.get(0).terminatePartial();
        assertThat((Long)rows.get(1).aggStates.get(0).value(), is(2L));
    }
}
