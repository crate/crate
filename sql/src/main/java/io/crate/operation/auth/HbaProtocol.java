package io.crate.operation.auth;

public enum HbaProtocol {

    POSTGRES("pg"),
    POSTGRES_SSL("pg"),
    HTTP("http");

    private final String protocolName;

    HbaProtocol(String protocolName) {
        this.protocolName = protocolName;
    }

    @Override
    public String toString() {
        return protocolName;
    }
}
