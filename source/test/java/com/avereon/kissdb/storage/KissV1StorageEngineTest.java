package com.avereon.kissdb.storage;

import com.avereon.kissdb.storage.model.TestDataTypeModel;
import com.avereon.kissdb.storage.model.TestIdentityModel;
import com.avereon.kissdb.storage.model.TestNoIdentityModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.io.IOUtil;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class KissV1StorageEngineTest {

	private final ObjectMapper MAPPER = new ObjectMapper();

	private String datastoreName;

	private String tableName;

	private Path storagePath;

	private Path tablePath;

	private Path datastorePath;

	private StorageEngine engine;

	@BeforeEach
	void setup() throws Exception {
		datastoreName = "testStore";
		tableName = "testTable";

		storagePath = Path.of( "target", "kissdbv1", "stores" );
		datastorePath = storagePath.resolve( datastoreName );
		tablePath = datastorePath.resolve( tableName );

		// TODO Remove all previous data

		// Create the storage engine
		engine = new KissDbV1StorageEngine( storagePath, datastoreName );

		// Start the storage engine
		engine.start();

		assertThat( engine.isRunning() ).isTrue();
		assertThat( storagePath ).exists();
		Files.createDirectories( tablePath );
	}

	@AfterEach
	void shutdown() throws Exception {
		if( engine != null ) engine.stop();
	}

	@Test
	void get() throws Exception {
		// given
		// Create a stored object file
		UUID id = UUID.randomUUID();
		TestIdentityModel data = new TestIdentityModel();
		data.setId( id );

		MAPPER.writeValue( tablePath.resolve( id.toString() ).toFile(), data );

		// when
		// Read the stored object file
		TestIdentityModel result = engine.get( tableName, id, TestIdentityModel.class );

		// then
		// Assert that the read object values are correct
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( data.getId() );
	}

	@Test
	void getWithNull() {
		// given
		UUID id = null;

		// when
		Throwable throwable = catchThrowable(() -> engine.get( tableName, id, TestIdentityModel.class ));

		// then
		assertThat( throwable ).isNotNull();
		assertThat( throwable ).isInstanceOf( StorageException.class );
		assertThat( throwable.getMessage() ).isEqualTo( "Id cannot be null" );
	}

	@Test
	void store() throws Exception {
		// given
		UUID id = UUID.randomUUID();
		TestIdentityModel data = new TestIdentityModel();
		data.setId( id );

		// when
		engine.store( tableName, data );
		TestIdentityModel result = engine.get( tableName, id, TestIdentityModel.class );

		// then
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( data.getId() );

		// Read the stored object file
		TestIdentityModel fileModel = MAPPER.readValue( tablePath.resolve( id.toString() ).toFile(), TestIdentityModel.class );
		assertThat( fileModel ).isNotNull();
		assertThat( fileModel.getId() ).isEqualTo( data.getId() );
	}

	@Test
	void storeWithExisting() throws Exception {
		// given
		UUID id = UUID.randomUUID();
		TestDataTypeModel data = new TestDataTypeModel();
		data.setId( id );
		data.setStringField( "original value" );
		engine.store( tableName, data );

		TestDataTypeModel initial = engine.get( tableName, id, TestDataTypeModel.class );
		assertThat( initial ).isNotNull();
		assertThat( initial.getId() ).isEqualTo( data.getId() );
		assertThat( initial.getStringField() ).isEqualTo( data.getStringField() );

		// when
		data.setStringField( "updated value" );
		engine.store( tableName, data );
		TestDataTypeModel result = engine.get( tableName, id, TestDataTypeModel.class );

		// then
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( data.getId() );
		assertThat( result.getStringField() ).isEqualTo( data.getStringField() );
		assertThat( result.getStringField() ).isNotEqualTo( initial.getStringField() );

		// Read the stored object file
		TestDataTypeModel fileModel = MAPPER.readValue( tablePath.resolve( id.toString() ).toFile(), TestDataTypeModel.class );
		assertThat( fileModel ).isNotNull();
		assertThat( fileModel.getId() ).isEqualTo( data.getId() );
		assertThat( fileModel.getStringField() ).isEqualTo( data.getStringField() );
		assertThat( fileModel.getStringField() ).isNotEqualTo( initial.getStringField() );
	}

	@Test
	void storeWithNull() {
		// given
		TestIdentityModel data = null;

		// when
		Throwable throwable = catchThrowable(() -> engine.store( tableName, data ) );

		// then
		assertThat( throwable ).isNotNull();
		assertThat( throwable ).isInstanceOf( StorageException.class );
		assertThat( throwable.getMessage() ).isEqualTo( "Object cannot be null" );
	}

	@Test
	void storeWithNoIdentity() {
		// given
		TestNoIdentityModel data = new TestNoIdentityModel();

		// when
		Throwable throwable = catchThrowable(() -> engine.store( tableName, data ) );

		// then
		assertThat( throwable ).isNotNull();
		assertThat( throwable ).isInstanceOf( StorageException.class );
		assertThat( throwable.getMessage() ).isEqualTo( "No @Id field found" );
	}

	@Test
	void remove() throws Exception {
		// given
		UUID id = UUID.randomUUID();
		TestIdentityModel data = new TestIdentityModel();
		data.setId( id );
		engine.store( tableName, data );

		TestIdentityModel initial = engine.get( tableName, id, TestIdentityModel.class );
		assertThat( initial ).isNotNull();
		assertThat( initial.getId() ).isEqualTo( data.getId() );

		// when
		engine.remove( tableName, id );
		Throwable throwable =  catchThrowable(() -> engine.get( tableName, id, TestIdentityModel.class ));

		// then
		assertThat( throwable ).isNotNull();
		assertThat( throwable ).isInstanceOf( StorageException.class );

		// Read the stored object file
		assertThat( tablePath.resolve( id.toString() ).toFile() ).doesNotExist();
	}

	@Test
	void removeWithNull() {
		// given
		UUID id = null;

		// when
		Throwable throwable = catchThrowable(() -> engine.remove( tableName, id ) );

		// then
		assertThat( throwable ).isNotNull();
		assertThat( throwable ).isInstanceOf( StorageException.class );
		assertThat( throwable.getMessage() ).isEqualTo( "Id cannot be null" );
	}

	@Test
	void getWithSupportedDataTypes() throws Exception {
		// given
		// Create a stored object file
		UUID id = UUID.randomUUID();
		TestDataTypeModel data = new TestDataTypeModel();
		data.setId( id );
		data.setStringField( "Test" );
		data.setIntField( 42 );
		data.setLongField( 1234567890L );
		data.setFloatField( 3.14f );
		data.setDoubleField( 2.71828 );
		data.setBooleanField( true );
		data.setUuidField( UUID.randomUUID() );

		String json = MAPPER.writeValueAsString( data );
		IOUtil.writeText( json, tablePath.resolve( id.toString() ).toFile() );

		// when
		// Read the stored object file
		TestDataTypeModel result = engine.get( tableName, id, TestDataTypeModel.class );

		// then
		// Assert that the read object values are correct
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( data.getId() );
		assertThat( result.getStringField() ).isEqualTo( data.getStringField() );
		assertThat( result.getIntField() ).isEqualTo( data.getIntField() );
		assertThat( result.getLongField() ).isEqualTo( data.getLongField() );
		assertThat( result.getFloatField() ).isEqualTo( data.getFloatField() );
		assertThat( result.getDoubleField() ).isEqualTo( data.getDoubleField() );
		assertThat( result.isBooleanField() ).isEqualTo( data.isBooleanField() );
		assertThat( result.getUuidField() ).isEqualTo( data.getUuidField() );
	}

	@Test
	void storeWithSupportedDataTypes() throws Exception {
		// given
		UUID id = UUID.randomUUID();
		TestDataTypeModel data = new TestDataTypeModel();
		data.setId( id );
		data.setStringField( "Test" );
		data.setIntField( 42 );
		data.setLongField( 1234567890L );
		data.setFloatField( 3.14f );
		data.setDoubleField( 2.71828 );
		data.setBooleanField( true );
		data.setUuidField( UUID.randomUUID() );

		// when
		TestDataTypeModel result = engine.store( tableName, data );

		// then
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( data.getId() );
		assertThat( result.getStringField() ).isEqualTo( data.getStringField() );
		assertThat( result.getIntField() ).isEqualTo( data.getIntField() );
		assertThat( result.getLongField() ).isEqualTo( data.getLongField() );
		assertThat( result.getFloatField() ).isEqualTo( data.getFloatField() );
		assertThat( result.getDoubleField() ).isEqualTo( data.getDoubleField() );
		assertThat( result.isBooleanField() ).isEqualTo( data.isBooleanField() );
		assertThat( result.getUuidField() ).isEqualTo( data.getUuidField() );

		// Read the stored object file
		TestDataTypeModel fileModel = MAPPER.readValue( tablePath.resolve( id.toString() ).toFile(), TestDataTypeModel.class );
		assertThat( fileModel ).isNotNull();
		assertThat( fileModel.getId() ).isEqualTo( data.getId() );
		assertThat( fileModel.getStringField() ).isEqualTo( data.getStringField() );
		assertThat( fileModel.getIntField() ).isEqualTo( data.getIntField() );
		assertThat( fileModel.getLongField() ).isEqualTo( data.getLongField() );
		assertThat( fileModel.getFloatField() ).isEqualTo( data.getFloatField() );
		assertThat( fileModel.getDoubleField() ).isEqualTo( data.getDoubleField() );
		assertThat( fileModel.isBooleanField() ).isEqualTo( data.isBooleanField() );
		assertThat( fileModel.getUuidField() ).isEqualTo( data.getUuidField() );
	}

}
