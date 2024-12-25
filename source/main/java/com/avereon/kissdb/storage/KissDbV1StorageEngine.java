package com.avereon.kissdb.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.persistence.Id;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
	public Map<String, Object> read( String table, UUID id ) throws StorageException {
		try {
			Path file = datastorePath.resolve( table ).resolve( id.toString() );
			return MAPPER.readValue( file.toFile(), new TypeReference<>() {} );
		} catch( IOException exception ) {
			throw new StorageException( "Unable to read object", exception );
		}
	}

	public <T> T read( String table, UUID id, Class<T> type ) throws StorageException {
		if( id == null ) throw new StorageException( "Id cannot be null" );

		try {
			return MAPPER.readValue( resolveObjectFile( table, id ), type );
		} catch( IOException exception ) {
			throw new StorageException( "Unable to read object", exception );
		}
	}

	@Override
	public <T> T upsert( String table, T object ) throws StorageException {
		if( object == null ) throw new StorageException( "Object cannot be null" );

		// Get the object id
		UUID id = getId( object );

		try {
			MAPPER.writeValue( resolveObjectFile( table, id ), object );
			return object;
		} catch( IOException exception ) {
			throw new StorageException( "Unable to read object", exception );
		}
	}

	@Override
	public void delete( String table, UUID id ) throws StorageException {
		if( id == null ) throw new StorageException( "Id cannot be null" );
		try {
			Files.deleteIfExists( resolveObjectPath( table, id ) );
		} catch( IOException exception ) {
			throw new StorageException( "Unable to delete object", exception );
		}
	}

	private UUID getId( Object object ) throws StorageException {
		for( Field field : getAllFields( object ) ) {
			if( field.isAnnotationPresent( Id.class ) ) {
				UUID id;
				field.setAccessible( true );
				try {
					id = (UUID)field.get( object );
					if( id == null ) {
						id = UUID.randomUUID();
						field.set( object, id );
					}
					return id;
				} catch( IllegalAccessException exception ) {
					throw new StorageException( "Unable to access @Id field", exception );
				}
			}
		}
		throw new StorageException( "No @Id field found" );
	}

	private Collection<Field> getAllFields( Object object ) {
		return getAllFields( object.getClass() );
	}

	private Collection<Field> getAllFields( Class<?> type ) {
		Collection<Field> fields = new ArrayList<>( List.of( type.getDeclaredFields() ) );
		Class<?> parent = type.getSuperclass();
		if( parent != null ) fields.addAll( getAllFields( parent ) );
		return fields;
	}

	private Path resolveObjectPath( String table, UUID id ) {
		return datastorePath.resolve( table ).resolve( id.toString() );
	}

	private File resolveObjectFile( String table, UUID id ) {
		return resolveObjectPath( table, id ).toFile();
	}

}
