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

package io.crate.protocols.ssl;

import io.crate.plugin.PipelineRegistry;
import io.netty.handler.ssl.SslContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.InvocationTargetException;

/**
 * Registers Netty's SslContext or provides it for dependency injection.
 * See PostgresWireProtocol
 * See PipelineRegistry
 */
@Singleton
public class SslContextProvider implements Provider<SslContext> {

    private static final String SSL_CONTEXT_CLAZZ = "io.crate.protocols.ssl.SslConfiguration";
    private static final String SSL_CONTEXT_METHOD_NAME = "buildSslContext";

    private SslContext sslContext;

    @SuppressWarnings("WeakerAccess")
    @Inject
    public SslContextProvider(Settings settings, PipelineRegistry pipelineRegistry) {
        if (SslConfigSettings.isSslEnabled(settings)) {
            this.sslContext = load(settings);
            if (SslConfigSettings.isHttpsEnabled(settings)) {
                pipelineRegistry.registerSslContext(sslContext);
            }
        }
    }

    private static SslContext load(Settings settings) {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        try {
            final Object value = classLoader
                .loadClass(SSL_CONTEXT_CLAZZ)
                .getDeclaredMethod(SSL_CONTEXT_METHOD_NAME, Settings.class)
                .invoke(null, settings);
            Class<SslContext> returnType = SslContext.class;
            if (!returnType.isAssignableFrom(value.getClass())) {
                throw new SslHandlerLoadingException("Returned type did not match the expected type: " + returnType);
            }
            //noinspection unchecked
            return (SslContext) value;
        } catch (Throwable e) {
            // The JVM wraps the exception of dynamically loaded classes into an InvocationTargetException
            // which we need to unpack first to see if we have an SslConfigurationException.
            tryUnwrapSslConfigurationException(e);
            throw new SslHandlerLoadingException(e);
        }
    }


    private static void tryUnwrapSslConfigurationException(Throwable e) {
        if (e instanceof InvocationTargetException) {
            Throwable cause = e.getCause();
            if (cause instanceof SslConfigurationException) {
                throw (SslConfigurationException) cause;
            }
        }
    }

    private static class SslHandlerLoadingException extends RuntimeException {

        SslHandlerLoadingException(String msg) {
            super(msg);
        }

        SslHandlerLoadingException(Throwable cause) {
            super("Loading the SslConfiguringHandler failed although enterprise is enabled.", cause);
        }
    }

    @Override
    public SslContext get() {
        return sslContext;
    }
}
