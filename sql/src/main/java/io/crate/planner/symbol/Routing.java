package io.crate.planner.symbol;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public class Routing implements Symbol {

    public static final SymbolFactory<Routing> FACTORY = new SymbolFactory<Routing>() {
        @Override
        public Routing newInstance() {
            return new Routing();
        }
    };

    public Routing() {

    }

    private Map<String, Map<String, Integer>> locations;

    public Routing(Map<String, Map<String, Integer>> locations) {
        this.locations = locations;
    }

    public Map<String, Map<String, Integer>> locations() {
        return locations;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.ROUTING;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitRouting(this, context);
    }

    public boolean hasLocations() {
        return locations != null && locations().size() > 0;
    }

    public Set<String> nodes() {
        if (hasLocations()) {
            return locations.keySet();
        }
        return ImmutableSet.of();
    }
}
