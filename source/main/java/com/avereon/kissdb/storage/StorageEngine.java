package com.avereon.kissdb.storage;

import java.util.UUID;

public interface StorageEngine {

	void start() throws StorageException;

	boolean isRunning();

	void stop() throws StorageException;

	<T> T read( String table, UUID id ) throws StorageException;

	<T> T upsert( String table, T object ) throws StorageException;

	<T> T delete( String table, UUID id ) throws StorageException;

}
