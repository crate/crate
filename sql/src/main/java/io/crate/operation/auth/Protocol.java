package io.crate.operation.auth;

public enum Protocol {

    POSTGRES("pg"),
    HTTP("http");

    private final String protocolName;

    Protocol(String protocolName) {
        this.protocolName = protocolName;
    }

    @Override
    public String toString() {
        return protocolName;
    }
}
