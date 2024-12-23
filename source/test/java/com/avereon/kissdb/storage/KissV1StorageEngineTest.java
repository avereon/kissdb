package com.avereon.kissdb.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class KissV1StorageEngineTest {

	private String datastoreName;

	private Path storagePath;

	private StorageEngine engine;

	@BeforeEach
	void setup() throws IOException {
		datastoreName = "test";
		storagePath = Path.of( "target", "kissdbv1", "storage" );

		// Create the storage engine
		engine = new KissDbV1StorageEngine( storagePath );

		// Start the storage engine
		engine.start();

		assertThat( engine.isRunning() ).isTrue();
		assertThat( storagePath ).exists();
	}

	@AfterEach
	void shutdown() throws IOException {
		if( engine != null ) engine.stop();
	}

	@Test
	void read() {
		// given
		// Create a stored object file

		// when
		// Read the stored object file

		// then
		// Assert that the read object values are correct
	}

	@Test
	void upsert() {

	}

	@Test
	void delete() {

	}

}
