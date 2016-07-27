package io.crate.operation.reference.sys.node.local;

import io.crate.monitor.ExtendedProcessCpuStats;
import io.crate.operation.reference.sys.node.SysNodeExpression;

class NodeProcessCpuExpression extends SysNodeObjectReference {

    private static final String PERCENT = "percent";
    private static final String USER = "user";
    private static final String SYSTEM = "system";

    NodeProcessCpuExpression(ExtendedProcessCpuStats cpuStats) {
        addChildImplementations(cpuStats);
    }

    private void addChildImplementations(final ExtendedProcessCpuStats cpuStats) {
        childImplementations.put(PERCENT, new SysNodeExpression<Short>() {
            @Override
            public Short value() {
                if (cpuStats != null) {
                    return cpuStats.percent();
                } else {
                    return -1;
                }
            }
        });
        childImplementations.put(USER, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                if (cpuStats != null) {
                    return cpuStats.user().millis();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(SYSTEM, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                if (cpuStats != null) {
                    return cpuStats.sys().millis();
                } else {
                    return -1L;
                }
            }
        });
    }
}
