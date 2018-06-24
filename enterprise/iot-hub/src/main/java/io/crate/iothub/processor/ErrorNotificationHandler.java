/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.iothub.processor;

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

import java.util.function.Consumer;

public class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs> {
    @Override
    public void accept(ExceptionReceivedEventArgs t) {
        System.out.println("Host " + t.getHostname() + " received general error notification during " + t.getAction() + ": " + t.getException().toString());
    }
}
