package com.avereon.kissdb.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

public class KissDbV1StorageEngine implements StorageEngine {

	private enum OperatingState {
		STARTING,
		STARTED,
		STOPPING,
		STOPPED
	}

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final Path datastorePath;

	private OperatingState state;

	public KissDbV1StorageEngine( Path storagePath, String datastoreName ) {
		this.datastorePath = storagePath.resolve( datastoreName );
	}

	@Override
	public void start() throws StorageException {
		try {
			state = OperatingState.STARTING;
			Files.createDirectories( datastorePath );
			state = OperatingState.STARTED;
		} catch( IOException exception ) {
			state = OperatingState.STOPPED;
			throw new StorageException( "Unable to start storage engine", exception );
		}
	}

	@Override
	public boolean isRunning() {
		return state == OperatingState.STARTED;
	}

	@Override
	public void stop() throws StorageException {
			state = OperatingState.STOPPED;
	}

	@Override
	@SuppressWarnings( "unchecked" )
	public Map<String,Object> read( String table, UUID id ) throws StorageException {
		try {
			Path file = datastorePath.resolve( table ).resolve( id.toString() );
			return MAPPER.readValue( file.toFile(), new TypeReference<>() {} );
		} catch( IOException exception ) {
			throw new StorageException( "Unable to read object", exception );
		}
	}

	public <T> T read( String table, UUID id, Class<T> type ) throws StorageException {
		// FIXME It is not correct to try and use the T value at runtime

		try {
			Path file = datastorePath.resolve( table ).resolve( id.toString() );
			return MAPPER.readValue( file.toFile(), type );
		} catch( IOException exception ) {
			throw new StorageException( "Unable to read object", exception );
		}
	}

	@Override
	public <T> T upsert( String table, T object ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T delete( String table, UUID id ) {
		// TODO Auto-generated method stub
		return null;
	}

}
