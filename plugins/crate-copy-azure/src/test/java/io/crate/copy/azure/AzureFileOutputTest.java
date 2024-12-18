/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.copy.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.net.URI;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class AzureFileOutputTest {

    @Test
    public void hardcoded_buffer_size_is_replaced() throws Exception {

        URI uri = URI.create("az://myaccount.blob.core.windows.net/container/file.json");
        Settings settings = Settings.builder().put("key", "dummy").build();
        SharedAsyncExecutor sharedAsyncExecutor = mock(SharedAsyncExecutor.class);
        when(sharedAsyncExecutor.asyncExecutor()).thenReturn(null);
        AzureFileOutput azureFileOutput = new AzureFileOutput(uri, sharedAsyncExecutor, settings);
        OutputStream outputStream = spy(azureFileOutput.acquireOutputStream(null, null));

        // OperatorOutputStream.MAX_BYTES is equal to 16384, we try to write more bytes
        // to ensure that constant is changed via reflection and flush is not called (ie now new block upload).
        int bytes = 16384 + 1;
        for (int i = 0; i < bytes; i++) {
            outputStream.write(1);
        }
        verify(outputStream, times(0)).flush();
    }

}
