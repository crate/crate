package io.crate.jobs;

import java.io.IOException;

public class ContextClosedException extends IOException {

    public ContextClosedException(){
        super("Can't start already closed context");
    }
}
