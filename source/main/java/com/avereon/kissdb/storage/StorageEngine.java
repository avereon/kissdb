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
	Map<String, Object> get( String table, UUID id ) throws StorageException;

	<T> T get( String table, UUID id, Class<T> type ) throws StorageException;

	<T> T store( String table, T object ) throws StorageException;

	void remove( String table, UUID id ) throws StorageException;

}
