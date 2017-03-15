package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Aggregation;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;


public class GroupingProjectorTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    /**
     * NOTE:
     * <p>
     * the remaining tests for the GroupingProjector are in {@link io.crate.operation.projectors.ProjectionToProjectorVisitorTest}
     **/

    @Test
    public void testAggregationToPartial() throws Exception {

        ImmutableList<Input<?>> keys = ImmutableList.of(
            new DummyInput(new BytesRef("one"), new BytesRef("one"), new BytesRef("three")));


        FunctionInfo countInfo = new FunctionInfo(new FunctionIdent("count", ImmutableList.of()), DataTypes.LONG);
        Aggregation countAggregation =
            Aggregation.partialAggregation(countInfo, DataTypes.LONG, ImmutableList.of());

        Functions functions = getFunctions();
        FunctionIdent ident = countInfo.ident();
        AggregationContext aggregationContext = new AggregationContext(
            (AggregationFunction) functions.get(ident.schema(), ident.name(), ident.argumentTypes()),
            countAggregation);

        AggregationContext[] aggregations = new AggregationContext[]{aggregationContext};
        GroupingProjector projector = new GroupingProjector(
            Arrays.asList(DataTypes.STRING),
            keys,
            new CollectExpression[0],
            aggregations,
            RAM_ACCOUNTING_CONTEXT
        );

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);

        Row emptyRow = new RowN(new Object[]{});

        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.finish(RepeatHandle.UNSUPPORTED);
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(2));
        assertThat(rows.iterator().next().get(1), instanceOf(CountAggregation.LongState.class));
    }

    class DummyInput implements Input<BytesRef> {

        private final BytesRef[] values;
        private int idx;

        DummyInput(BytesRef... values) {
            this.values = values;
            this.idx = 0;
        }

        @Override
        public BytesRef value() {
            return values[idx++];
        }
    }
}
