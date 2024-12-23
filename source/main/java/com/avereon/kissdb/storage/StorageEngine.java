package com.avereon.kissdb.storage;

import java.io.IOException;
import java.util.UUID;

public interface StorageEngine {

    void start() throws IOException;

    boolean isRunning();

    void stop() throws IOException;

    void read(UUID id);

    <T> void upsert(T object);

    void delete(UUID id);
}
