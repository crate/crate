package io.crate.execution.engine.aggregation.impl;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.breaker.MemoryCircuitBreaker;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class CollectionSumAggregationTest {

    private RamAccounting ramAccounting = RamAccounting.NO_ACCOUNTING;
    private MemoryManager memoryManager = new MemoryManager(
        new MemoryCircuitBreaker(Settings.EMPTY, "test"),
        MemorySizeValue.memorySizeValue("0b")
    );

    private CollectionSumAggregation createAgg() {
        return new CollectionSumAggregation(
            Signature.builder("collection_sum", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING)
                .returnType(DataTypes.LONG)
                .build(),
            new BoundSignature("collection_sum", List.of(DataTypes.STRING), DataTypes.LONG)
        );
    }

    @Test
    public void test_sum_distinct_values() throws CircuitBreakingException {
        CollectionSumAggregation agg = createAgg();

        @SuppressWarnings("unchecked")
        Set<Number> set = new HashSet<>();
        set.add(1L);
        set.add(2L);
        set.add(3L);
        set.add(2L); // duplicate - should be ignored

        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        Number result = agg.terminatePartial(ramAccounting, state);

        assertEquals(6L, result.longValue());
    }

    @Test
    public void test_sum_distinct_empty_set() throws CircuitBreakingException {
        CollectionSumAggregation agg = createAgg();

        Set<Number> set = new HashSet<>();
        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        Number result = agg.terminatePartial(ramAccounting, state);

        assertEquals(0L, result.longValue());
    }
}
