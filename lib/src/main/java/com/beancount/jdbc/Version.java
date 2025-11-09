package com.beancount.jdbc;

public final class Version {
    static final int MAJOR = 0;
    static final int MINOR = 1;
    static final int PATCH = 0;

    public static final String FULL = MAJOR + "." + MINOR + "." + PATCH + "-beta";

    private Version() {}
}
