package io.crate.expression.scalar;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

import java.util.List;
import java.util.Objects;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class ArrayPositionFunction extends Scalar<Integer, List<Object>> {

    public static final String NAME = "array_position";
    private final Signature signature;
    private final Signature boundSignature;

    public ArrayPositionFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        ensureInnerTypeIsNotUndefined(boundSignature.getArgumentDataTypes(), signature.getName().name());
    }

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(
            Signature.scalar(NAME,
                parseTypeSignature("array(T)"),
                parseTypeSignature("T"),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("T")),
            ArrayPositionFunction::new);

        scalarFunctionModule.register(
            Signature.scalar(NAME,
                parseTypeSignature("array(T)"),
                parseTypeSignature("T"),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("T")),
            ArrayPositionFunction::new);
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }


    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input[] args) {

        List<Object> elements = (List<Object>) args[0].value();
        if (elements == null || elements.isEmpty()) {
            return null;
        }

        Object targetValue = args[1].value();

        //Start iteration with 0 if optional parameter not supplied
        Integer beginIndex = 0;
        if (args.length > 2) {
            beginIndex = getBeginPosition(args[2].value(), elements.size());
        }

        if (beginIndex == null) {
            return null;
        }

        Object element = null;
        for (int i = beginIndex; i < elements.size(); i++) {
            element = elements.get(i);
            if (Objects.equals(targetValue, element)) {
                return i + 1; //Return position.
            }
        }

        return null;
    }

    private Integer getBeginPosition(Object position, int elementsSize) {

        if (position == null) {
            return 0;
        }

        Integer beginPosition = (Integer) position;
        if (beginPosition < 1 || beginPosition > elementsSize) {
            return null;
        }

        return beginPosition - 1;
    }
}
