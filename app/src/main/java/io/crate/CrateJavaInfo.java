/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate;

import com.google.common.annotations.VisibleForTesting;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Arrays;

public class CrateJavaInfo extends Command {

    public static void main(final String[] args) throws Exception {
        final CrateJavaInfo crate = new CrateJavaInfo();
        int status = crate.main(args, Terminal.DEFAULT);
        if (status != ExitCodes.OK) {
            exit(status);
        }
    }

    private final OptionSpec<Void> maxHeapOption;

    @VisibleForTesting
    CrateJavaInfo() {
        super("get information about the java process");
        this.maxHeapOption = parser.acceptsAll(Arrays.asList("m", "maxHeapSize"),
            "Print out the maximum configured HEAP size (-Xmx)");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        boolean showMaxHeap = options.has(maxHeapOption);
        if (showMaxHeap) {
            terminal.println(String.format("%d", JvmInfo.jvmInfo().getConfiguredMaxHeapSize()));
        }
    }
}
