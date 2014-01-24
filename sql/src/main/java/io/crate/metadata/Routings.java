package io.crate.metadata;

public interface Routings {
    Routing getRouting(TableIdent tableIdent);
}
