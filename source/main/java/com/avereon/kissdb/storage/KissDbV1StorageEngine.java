package com.avereon.kissdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class KissDbV1StorageEngine implements StorageEngine {

	private final Path storagePath;

	private boolean running;

	public KissDbV1StorageEngine( Path storagePath ) {
		this.storagePath = storagePath;
	}

	@Override
	public void start() throws IOException {
		Files.createDirectories( storagePath );
		running = true;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public void stop() throws IOException {
		running = false;
	}

	@Override
	public void read( UUID id ) {
		// TODO Auto-generated method stub
	}

	@Override
	public <T> void upsert( T object ) {
		// TODO Auto-generated method stub
	}

	@Override
	public void delete( UUID id ) {
		// TODO Auto-generated method stub
	}

}
