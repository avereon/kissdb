package com.avereon.kissdb.storage.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Instant;
import java.util.UUID;

@Data
@EqualsAndHashCode( callSuper = true )
public class TestDataTypeModel extends TestBaseDataModel {

	private String stringField;

	private int intField;

	private long longField;

	private float floatField;

	private double doubleField;

	private boolean booleanField;

	private UUID uuidField;

	private Instant instantField;

}

