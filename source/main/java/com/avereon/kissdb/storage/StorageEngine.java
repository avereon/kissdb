package com.avereon.kissdb.storage;

import java.util.Map;
import java.util.UUID;

public interface StorageEngine {

	void start() throws StorageException;

	boolean isRunning();

	void stop() throws StorageException;

	/**
	 * Read and object as a {@link Map}. This is a low level method to retrieve
	 * any object as a simple map.
	 *
	 * @param table The table the object is stored in
	 * @param id The object id
	 * @return The object as a {@link Map}
	 * @throws StorageException If anything goes wrong
	 */
	Map<String, Object> read( String table, UUID id ) throws StorageException;

	<T> T read( String table, UUID id, Class<T> type ) throws StorageException;

	<T> T upsert( String table, T object ) throws StorageException;

	<T> T delete( String table, UUID id ) throws StorageException;

}
