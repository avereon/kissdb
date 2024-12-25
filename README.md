# kissdb

# Keep It Stupid Simple Datastore Engine

KissDb is a simple object data storage library written in Java. It is intended 
to be a simple way to store and retrieve objects in datastore files. The engine
stores objects in JSON formatted files. The database is thread-safe, and will 
ensure that asynchronous write operations will not result in corrupt data.

## Premise

Data storage is hard, but doesn't need to be. KissDB is a simple storage engine
that allows you to store and retrieve objects in a simple and easy way.

## Usage

```java
import com.avereon.kissdb.StorageEngine;
import com.avereon.kissdb.KissDbV1StorageEngine;

public class Example {

	public static void main( String[] args ) {
		StorageEngine engine = new KissDbV1StorageEngine( "example" );

		// Start the engine		
		engine.start();

		// Create an object to store
		Person person = new Person();
		person.setName( "John Doe" );

		// Store an object
		person = engine.store( "person", person );

		// Retrieve an object
		Person value = engine.get( "person", person.getId(), Person.class );
		System.out.println( value );

		// Remove an object
		engine.remove( "person", person.getId() );

		// Stop the engine
		engine.stop();
	}

}

```
