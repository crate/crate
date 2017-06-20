package io.crate.operation.auth;

public enum HbaProtocol {

    POSTGRES("pg", false),
    POSTGRES_SSL("pg", true),
    HTTP("http", false),
    HTTPS("http", true);

    private final String protocolName;
    private final boolean usesSsl;

    HbaProtocol(String protocolName, boolean usesSsl) {
        this.protocolName = protocolName;
        this.usesSsl = usesSsl;
    }

    public boolean isSslProtocol() {
        return this.usesSsl;
    }

    @Override
    public String toString() {
        return protocolName;
    }
}
