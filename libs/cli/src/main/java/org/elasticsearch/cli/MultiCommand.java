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

package org.elasticsearch.cli;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionSet;

/**
 * A cli tool which is made up of multiple subcommands.
 */
public class MultiCommand extends Command {

    protected final Map<String, Command> subcommands = new LinkedHashMap<>();

    private final NonOptionArgumentSpec<String> arguments = parser.nonOptions("command");

    /**
     * Construct the multi-command with the specified command description and runnable to execute before main is invoked.
     *
     * @param description the multi-command description
     * @param beforeMain the before-main runnable
     */
    public MultiCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
        parser.posixlyCorrect(true);
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }
        terminal.println("Commands");
        terminal.println("--------");
        for (Map.Entry<String, Command> subcommand : subcommands.entrySet()) {
            Command value = subcommand.getValue();
            terminal.println(subcommand.getKey() + " - " + value.description);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }
        String[] args = arguments.values(options).toArray(new String[0]);
        if (args.length == 0) {
            throw new UserException(ExitCodes.USAGE, "Missing command");
        }
        Command subcommand = subcommands.get(args[0]);
        if (subcommand == null) {
            throw new UserException(ExitCodes.USAGE, "Unknown command [" + args[0] + "]");
        }
        subcommand.mainWithoutErrorHandling(Arrays.copyOfRange(args, 1, args.length), terminal);
    }

    @Override
    public void close() throws IOException {
        close(subcommands.values());
    }

    /**
     * Closes all given {@link Closeable}s. Some of the {@linkplain Closeable}s may be null; they are
     * ignored. After everything is closed, the method either throws the first exception it hit
     * while closing with other exceptions added as suppressed, or completes normally if there were
     * no exceptions.
     *
     * @param objects objects to close
     */
    public static void close(final Iterable<? extends Closeable> objects) throws IOException {
        close(null, objects);
    }

    /**
     * Closes all given {@link Closeable}s. If a non-null exception is passed in, or closing a
     * stream causes an exception, throws the exception with other {@link RuntimeException} or
     * {@link IOException} exceptions added as suppressed.
     *
     * @param ex existing Exception to add exceptions occurring during close to
     * @param objects objects to close
     */
    public static void close(final Exception ex, final Iterable<? extends Closeable> objects) throws IOException {
        Exception firstException = ex;
        for (final Closeable object : objects) {
            try {
                if (object != null) {
                    object.close();
                }
            } catch (final IOException | RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else {
                // since we only assigned an IOException or a RuntimeException to ex above, in this case ex must be a RuntimeException
                throw (RuntimeException) firstException;
            }
        }
    }
}
