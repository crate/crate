package io.crate.operator.reference.sys;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.sys.SysExpression;

import java.util.HashMap;
import java.util.Map;

public abstract class SysObjectReference<ChildType> extends SysExpression<Map<String, ChildType>>
        implements ReferenceImplementation {

    protected final Map<String, SysExpression<ChildType>> childImplementations = new HashMap<>();

    @Override
    public SysExpression<ChildType> getChildImplementation(String name) {
        return childImplementations.get(name);
    }

    @Override
    public Map<String, ChildType> value() {
        ImmutableMap.Builder<String, ChildType> builder = ImmutableMap.builder();
        for (Map.Entry<String, SysExpression<ChildType>> e : childImplementations.entrySet()) {
            builder.put(e.getKey(), e.getValue().value());
        }
        return builder.build();
    }


}
