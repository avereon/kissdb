package com.avereon.kissdb.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.io.IOUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
		tablePath = datastorePath.resolve(tableName);

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
	void read() throws Exception {
		// given
		// Create a stored object file
		UUID id = UUID.randomUUID();
		BaseTestDataModel data = new BaseTestDataModel();
		data.setId( id );
		String json = MAPPER.writeValueAsString( data );
		IOUtil.writeText( json, tablePath.resolve( id.toString() ).toFile() );

		// when
		// Read the stored object file
		BaseTestDataModel result = engine.read( tableName, id, BaseTestDataModel.class );

		// then
		// Assert that the read object values are correct
		assertThat( result ).isNotNull();
		assertThat( result.getId() ).isEqualTo( id );
	}

	@Test
	void upsert() {

	}

	@Test
	void delete() {

	}

}
