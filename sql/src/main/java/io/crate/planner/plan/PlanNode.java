package io.crate.planner.plan;

// PRESTOBORROW


import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class PlanNode implements Streamable {

    private String id;
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

    public void sources(List<PlanNode> sources) {
        this.sources = sources;
    }

    public List<PlanNode> sources() {
        return sources;
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readString();

        // TODO:
        // int numSymbols = in.readVInt();
        // symbols = new ArrayList<>(numSymbols);
        // for (int i = 0; i < numSymbols; i++) {
        //     symbols.add(SymbolType.readSymbol(in));
        // }

        // numSymbols = in.readVInt();
        // inputs = new ArrayList<>(numSymbols);
        // for (int i = 0; i < numSymbols; i++) {
        //     inputs.add(SymbolType.readSymbol(in));
        // }

        // numSymbols = in.readVInt();
        // outputs = new ArrayList<>(numSymbols);
        // for (int i = 0; i < numSymbols; i++) {
        //     outputs.add(SymbolType.readSymbol(in));
        // }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);

        // TODO:
        // out.writeVInt(symbols.size());
        // for (Symbol symbol : symbols) {
        //     symbol.writeTo(out);
        // }

        // out.writeVInt(inputs.size());
        // for (Symbol input : inputs) {
        //     input.writeTo(out);
        // }

        // out.writeVInt(outputs.size());
        // for (Symbol output : outputs) {
        //     output.writeTo(out);
        // }
    }
}
