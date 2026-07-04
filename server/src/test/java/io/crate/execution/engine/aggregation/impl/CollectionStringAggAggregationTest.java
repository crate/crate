package io.crate.execution.engine.aggregation.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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

public class CollectionStringAggAggregationTest {

    private RamAccounting ramAccounting = RamAccounting.NO_ACCOUNTING;
    private MemoryManager memoryManager = new MemoryManager(
        new MemoryCircuitBreaker(Settings.EMPTY, "test"),
        MemorySizeValue.memorySizeValue("0b")
    );

    private CollectionStringAggAggregation createAgg() {
        return new CollectionStringAggAggregation(
            Signature.builder("collection_string_agg", FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING, DataTypes.STRING)
                .returnType(DataTypes.STRING)
                .build(),
            new BoundSignature("collection_string_agg", List.of(DataTypes.STRING, DataTypes.STRING), DataTypes.STRING)
        );
    }

    @Test
    public void test_string_agg_distinct_values() throws CircuitBreakingException {
        CollectionStringAggAggregation agg = createAgg();

        Set<String> set = new HashSet<>();
        set.add("hello");
        set.add("world");
        set.add("hello"); // duplicate

        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        String result = agg.terminatePartial(ramAccounting, state);

        // Order may vary in HashSet
        assert result != null;
        assert result.contains("hello");
        assert result.contains("world");
    }

    @Test
    public void test_string_agg_empty_set() throws CircuitBreakingException {
        CollectionStringAggAggregation agg = createAgg();

        Set<String> set = new HashSet<>();
        Input<Object> input = () -> set;

        Object state = agg.newState(ramAccounting, Version.CURRENT, memoryManager);
        state = agg.iterate(ramAccounting, memoryManager, state, input);
        String result = agg.terminatePartial(ramAccounting, state);

        assertNull(result);
    }
}
