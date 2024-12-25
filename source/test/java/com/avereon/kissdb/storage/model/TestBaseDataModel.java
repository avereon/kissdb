package com.avereon.kissdb.storage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import javax.persistence.Id;
import java.util.UUID;

@Data
@JsonInclude( JsonInclude.Include.NON_NULL )
public abstract class TestBaseDataModel {

	@Id
	private UUID id;

}
