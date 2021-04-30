# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

# check in case a user was using this mechanism
if [ "x$CRATE_CLASSPATH" != "x" ]; then
    cat >&2 << EOF
Error: Don't modify the classpath with CRATE_CLASSPATH.
Add plugins and their dependencies into the plugins/ folder instead.
EOF
    exit 1
fi

for libname in "$CRATE_HOME"/lib/*.jar; do
    if [ "x$CRATE_CLASSPATH" != "x" ]; then
        CRATE_CLASSPATH="$CRATE_CLASSPATH:$libname"
    else
        CRATE_CLASSPATH="$libname"
    fi
done

if [ "x$CRATE_MIN_MEM" = "x" ]; then
    CRATE_MIN_MEM=256m
fi
if [ "x$CRATE_HEAP_SIZE" != "x" ]; then
    CRATE_MIN_MEM=$CRATE_HEAP_SIZE
    CRATE_MAX_MEM=$CRATE_HEAP_SIZE
fi

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JAVA_OPTS="$JAVA_OPTS -Xms${CRATE_MIN_MEM}"
if [ "x$CRATE_MAX_MEM" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmx${CRATE_MAX_MEM}"
fi

# new generation
if [ "x$CRATE_HEAP_NEWSIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmn${CRATE_HEAP_NEWSIZE}"
fi

# max direct memory
if [ "x$CRATE_DIRECT_SIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=${CRATE_DIRECT_SIZE}"
fi

# set to headless, just in case
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"

# Force the JVM to use IPv4 stack
if [ "x$CRATE_USE_IPV4" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"
fi

## GC configuration
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30"

# GC logging options
# Set CRATE_DISABLE_GC_LOGGING=1 to disable GC logging
if [ "x$CRATE_DISABLE_GC_LOGGING" = "x" ]; then
  # GC log directory needs to be set explicitly by packages
  # GC logging requires 16x64mb = 1g of free disk space
  GC_LOG_DIR=${CRATE_GC_LOG_DIR:-"$CRATE_HOME/logs"};
  GC_LOG_SIZE=${CRATE_GC_LOG_SIZE:-"64m"}
  GC_LOG_FILES=${CRATE_GC_LOG_FILES:-"16"}

  # Ensure that the directory for the log file exists: the JVM will not create it.
  if (! test -d "$GC_LOG_DIR" || ! test -x "$GC_LOG_DIR"); then
    cat >&2 << EOF
ERROR: Garbage collection log directory '$GC_LOG_DIR' does not exist or is not accessible.
EOF
    exit 1
  fi
  LOGGC="$GC_LOG_DIR/gc.log"

  if [ -x "$JAVA_HOME/bin/java" ]; then
      JAVA="$JAVA_HOME/bin/java"
  else
      JAVA=java
  fi
  JAVA_OPTS="$JAVA_OPTS -Xlog:gc*,gc+age=trace,safepoint:file=\"${LOGGC}\":utctime,pid,tags:filecount=${GC_LOG_FILES},filesize=${GC_LOG_SIZE}"
fi

# Disables explicit GC
JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"

# Ensure UTF-8 encoding by default (e.g. filenames)
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"

# Use our provided JNA always versus the system one
JAVA_OPTS="$JAVA_OPTS -Djna.nosys=true"

# log4j options
JAVA_OPTS="$JAVA_OPTS -Dlog4j.shutdownHookEnabled=false -Dlog4j2.disable.jmx=true -Dlog4j.skipJansi=true"

# Disable netty recycler
JAVA_OPTS="$JAVA_OPTS -Dio.netty.recycler.maxCapacityPerThread=0"

# Dump heap on OOM
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
if [ "x$CRATE_HEAP_DUMP_PATH" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$CRATE_HEAP_DUMP_PATH"
fi
