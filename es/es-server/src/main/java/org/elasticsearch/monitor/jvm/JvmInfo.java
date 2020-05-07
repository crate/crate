/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JvmInfo implements Writeable {

    private static JvmInfo INSTANCE;

    static {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        long heapInit = memoryMXBean.getHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getInit();
        long heapMax = memoryMXBean.getHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getMax();
        final String[] inputArguments = runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]);
        final Mem mem = new Mem(heapInit, heapMax);

        String bootClassPath;
        try {
            bootClassPath = runtimeMXBean.getBootClassPath();
        } catch (UnsupportedOperationException e) {
            // oracle java 9
            bootClassPath = System.getProperty("sun.boot.class.path");
            if (bootClassPath == null) {
                // something else
                bootClassPath = "<unknown>";
            }
        }
        final String classPath = runtimeMXBean.getClassPath();
        final Map<String, String> systemProperties = Collections.unmodifiableMap(runtimeMXBean.getSystemProperties());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        String[] gcCollectors = new String[gcMxBeans.size()];
        for (int i = 0; i < gcMxBeans.size(); i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            gcCollectors[i] = gcMxBean.getName();
        }

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        String[] memoryPools = new String[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            memoryPools[i] = memoryPoolMXBean.getName();
        }

        String onError = null;
        String onOutOfMemoryError = null;
        String useCompressedOops = "unknown";
        String useG1GC = "unknown";
        String useSerialGC = "unknown";
        long configuredInitialHeapSize = -1;
        long configuredMaxHeapSize = -1;
        try {
            @SuppressWarnings("unchecked") Class<? extends PlatformManagedObject> clazz =
                    (Class<? extends PlatformManagedObject>)Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            Class<?> vmOptionClazz = Class.forName("com.sun.management.VMOption");
            PlatformManagedObject hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(clazz);
            Method vmOptionMethod = clazz.getMethod("getVMOption", String.class);
            Method valueMethod = vmOptionClazz.getMethod("getValue");

            try {
                Object onErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnError");
                onError = (String) valueMethod.invoke(onErrorObject);
            } catch (Exception ignored) {
            }

            try {
                Object onOutOfMemoryErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnOutOfMemoryError");
                onOutOfMemoryError = (String) valueMethod.invoke(onOutOfMemoryErrorObject);
            } catch (Exception ignored) {
            }

            try {
                Object useCompressedOopsVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseCompressedOops");
                useCompressedOops = (String) valueMethod.invoke(useCompressedOopsVmOptionObject);
            } catch (Exception ignored) {
            }

            try {
                Object useG1GCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseG1GC");
                useG1GC = (String) valueMethod.invoke(useG1GCVmOptionObject);
            } catch (Exception ignored) {
            }

            try {
                Object initialHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "InitialHeapSize");
                configuredInitialHeapSize = Long.parseLong((String) valueMethod.invoke(initialHeapSizeVmOptionObject));
            } catch (Exception ignored) {
            }

            try {
                Object maxHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "MaxHeapSize");
                configuredMaxHeapSize = Long.parseLong((String) valueMethod.invoke(maxHeapSizeVmOptionObject));
            } catch (Exception ignored) {
            }

            try {
                Object useSerialGCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseSerialGC");
                useSerialGC = (String) valueMethod.invoke(useSerialGCVmOptionObject);
            } catch (Exception ignored) {
            }

        } catch (Exception ignored) {

        }

        INSTANCE = new JvmInfo(
            JvmPid.getPid(),
            System.getProperty("java.version"),
            runtimeMXBean.getVmName(),
            runtimeMXBean.getVmVersion(),
            runtimeMXBean.getVmVendor(),
            runtimeMXBean.getStartTime(),
            configuredInitialHeapSize,
            configuredMaxHeapSize,
            mem,
            inputArguments,
            bootClassPath,
            classPath,
            systemProperties,
            gcCollectors,
            memoryPools,
            onError,
            onOutOfMemoryError,
            useCompressedOops,
            useG1GC,
            useSerialGC
        );
    }

    public static JvmInfo jvmInfo() {
        return INSTANCE;
    }

    private final long pid;
    private final String version;
    private final String vmName;
    private final String vmVersion;
    private final String vmVendor;
    private final long startTime;
    private final long configuredInitialHeapSize;
    private final long configuredMaxHeapSize;
    private final Mem mem;
    private final String[] inputArguments;
    private final String bootClassPath;
    private final String classPath;
    private final Map<String, String> systemProperties;
    private final String[] gcCollectors;
    private final String[] memoryPools;
    private final String onError;
    private final String onOutOfMemoryError;
    private final String useCompressedOops;
    private final String useG1GC;
    private final String useSerialGC;

    private JvmInfo(long pid, String version, String vmName, String vmVersion, String vmVendor, long startTime,
                   long configuredInitialHeapSize, long configuredMaxHeapSize, Mem mem, String[] inputArguments, String bootClassPath,
                   String classPath, Map<String, String> systemProperties, String[] gcCollectors, String[] memoryPools, String onError,
                   String onOutOfMemoryError, String useCompressedOops, String useG1GC, String useSerialGC) {
        this.pid = pid;
        this.version = version;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.startTime = startTime;
        this.configuredInitialHeapSize = configuredInitialHeapSize;
        this.configuredMaxHeapSize = configuredMaxHeapSize;
        this.mem = mem;
        this.inputArguments = inputArguments;
        this.bootClassPath = bootClassPath;
        this.classPath = classPath;
        this.systemProperties = systemProperties;
        this.gcCollectors = gcCollectors;
        this.memoryPools = memoryPools;
        this.onError = onError;
        this.onOutOfMemoryError = onOutOfMemoryError;
        this.useCompressedOops = useCompressedOops;
        this.useG1GC = useG1GC;
        this.useSerialGC = useSerialGC;
    }

    public JvmInfo(StreamInput in) throws IOException {
        pid = in.readLong();
        version = in.readString();
        vmName = in.readString();
        vmVersion = in.readString();
        vmVendor = in.readString();
        startTime = in.readLong();
        inputArguments = new String[in.readInt()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readString();
        }
        bootClassPath = in.readString();
        classPath = in.readString();
        systemProperties = in.readMap(StreamInput::readString, StreamInput::readString);
        mem = new Mem(in);
        gcCollectors = in.readStringArray();
        memoryPools = in.readStringArray();
        useCompressedOops = in.readString();
        //the following members are only used locally for bootstrap checks, never serialized nor printed out
        this.configuredMaxHeapSize = -1;
        this.configuredInitialHeapSize = -1;
        this.onError = null;
        this.onOutOfMemoryError = null;
        this.useG1GC = "unknown";
        this.useSerialGC = "unknown";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(pid);
        out.writeString(version);
        out.writeString(vmName);
        out.writeString(vmVersion);
        out.writeString(vmVendor);
        out.writeLong(startTime);
        out.writeInt(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeString(inputArgument);
        }
        out.writeString(bootClassPath);
        out.writeString(classPath);
        out.writeVInt(this.systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        mem.writeTo(out);
        out.writeStringArray(gcCollectors);
        out.writeStringArray(memoryPools);
        out.writeString(useCompressedOops);
    }

    /**
     * The process id.
     */
    public long pid() {
        return this.pid;
    }

    public String version() {
        return this.version;
    }

    public String getVmName() {
        return this.vmName;
    }

    public Mem getMem() {
        return this.mem;
    }

    public String[] getInputArguments() {
        return this.inputArguments;
    }

    public long getConfiguredInitialHeapSize() {
        return configuredInitialHeapSize;
    }

    public long getConfiguredMaxHeapSize() {
        return configuredMaxHeapSize;
    }

    public String onError() {
        return onError;
    }

    public String onOutOfMemoryError() {
        return onOutOfMemoryError;
    }

    /**
     * The value of the JVM flag UseCompressedOops, if available otherwise
     * "unknown". The value "unknown" indicates that an attempt was
     * made to obtain the value of the flag on this JVM and the attempt
     * failed.
     *
     * @return the value of the JVM flag UseCompressedOops or "unknown"
     */
    public String useCompressedOops() {
        return this.useCompressedOops;
    }

    public String useG1GC() {
        return this.useG1GC;
    }

    public String useSerialGC() {
        return this.useSerialGC;
    }

    public static class Mem implements Writeable {

        private final long heapInit;
        private final long heapMax;

        Mem(long heapInit, long heapMax) {
            this.heapInit = heapInit;
            this.heapMax = heapMax;
        }

        Mem(StreamInput in) throws IOException {
            this.heapInit = in.readVLong();
            this.heapMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapInit);
            out.writeVLong(heapMax);
        }

        public ByteSizeValue getHeapInit() {
            return new ByteSizeValue(heapInit);
        }

        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }
    }
}
