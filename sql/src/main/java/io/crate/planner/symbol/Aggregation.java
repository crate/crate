package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionIdent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Aggregation extends Symbol {

    public static final SymbolFactory<Aggregation> FACTORY = new SymbolFactory<Aggregation>() {
        @Override
        public Aggregation newInstance() {
            return new Aggregation();
        }
    };

    public Aggregation() {

    }

    public static enum Step {
        ITER, PARTIAL, FINAL;

        static void writeTo(Step step, StreamOutput out) throws IOException {
            out.writeVInt(step.ordinal());
        }

        static Step readFrom(StreamInput in) throws IOException {
            return values()[in.readVInt()];
        }
    }

    private FunctionIdent functionIdent;
    private List<ValueSymbol> inputs;
    private Step fromStep;
    private Step toStep;

    public Aggregation(FunctionIdent functionIdent, List<ValueSymbol> inputs, Step fromStep, Step toStep) {
        Preconditions.checkNotNull(inputs);
        this.functionIdent = functionIdent;
        this.inputs = inputs;
        this.fromStep = fromStep;
        this.toStep = toStep;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.AGGREGATION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

    public FunctionIdent functionIdent() {
        return functionIdent;
    }

    public List<ValueSymbol> inputs() {
        return inputs;
    }

    public Step fromStep() {
        return fromStep;
    }


    public Step toStep() {
        return toStep;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        functionIdent = new FunctionIdent();
        functionIdent.readFrom(in);

        fromStep = Step.readFrom(in);
        toStep = Step.readFrom(in);

        int numInputs = in.readVInt();
        inputs = new ArrayList<>(numInputs);
        for (int i = 0; i < numInputs; i++) {
            inputs.add((ValueSymbol)Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        functionIdent.writeTo(out);

        Step.writeTo(fromStep, out);
        Step.writeTo(toStep, out);

        out.writeVInt(inputs.size());
        for (ValueSymbol input : inputs) {
            Symbol.toStream(input, out);
        }
    }
}
