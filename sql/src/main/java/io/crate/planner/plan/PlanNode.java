package io.crate.planner.plan;

// PRESTOBORROW


import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.List;

public abstract class PlanNode {

    private final String id;
    private List<PlanNode> sources;
    private List<PlanNode> targets;
    private List<Symbol> symbols;

    private List<Symbol> inputs;
    private List<Symbol> outputs;

    protected PlanNode(String id) {
        this.id = id;
    }

    protected PlanNode(String id, List<PlanNode> sources, List<PlanNode> targets) {
        this(id);
        this.sources = sources;
        this.targets = targets;
    }

    protected PlanNode(String id, PlanNode source, PlanNode target) {
        this(id, ImmutableList.of(source), ImmutableList.of(target));
    }

    public String id() {
        return id;
    }

    public void source(PlanNode source) {
        this.sources = ImmutableList.of(source);
    }

    public void target(PlanNode target) {
        this.sources = ImmutableList.of(target);
    }

    public void sources(List<PlanNode> sources) {
        this.sources = sources;
    }

    public void targets(List<PlanNode> targets) {
        this.targets = targets;
    }

    public List<PlanNode> sources() {
        return sources;
    }

    public List<PlanNode> targets() {
        return targets;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitPlan(this, context);
    }

    public void symbols(Symbol... symbols) {
        // TODO: check if this is possible without copy
        this.symbols = ImmutableList.copyOf(symbols);
    }

    public void inputs(Symbol... inputs) {
        this.inputs = ImmutableList.copyOf(inputs);
    }

    public void outputs(Symbol... outputs) {
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public String getId() {
        return id;
    }

    public List<Symbol> symbols() {
        return symbols;
    }

    public List<Symbol> inputs() {
        return inputs;
    }

    public List<Symbol> outputs() {
        return outputs;
    }
}
