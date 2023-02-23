/*
 * This file is part of RDF Federator.
 * Copyright 2011 Olaf Goerlitz
 * 
 * RDF Federator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * RDF Federator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with RDF Federator.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * RDF Federator uses libraries from the OpenRDF Sesame Project licensed 
 * under the Aduna BSD-style license. 
 */
package de.uni_koblenz.west.splendid;

import java.io.File;
import java.io.IOException;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.config.VoidRepositoryConfig;
import de.uni_koblenz.west.splendid.statistics.VoidStatistics;

/**
 * A proxy for a remote repository which is accessed via a SPARQL endpoint.
 * 
 * @author Olaf Goerlitz
 */
public class VoidRepository implements Repository {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VoidRepository.class);
	
	protected final ValueFactory vf = new ValueFactoryImpl();
	protected IRI endpoint;
	protected final IRI voidIRI;
	
	protected boolean initialized = false;
	
	public VoidRepository(VoidRepositoryConfig config) {
		this.endpoint = config.getEndpoint();
		this.voidIRI = config.getVoidIRI();
	}
	
	public IRI getEndpoint() {
		return this.endpoint;
	}

	// --------------------------------------------------------------
	
	// Dummy
	@Override
	public boolean isInitialized(){
		return this.initialized;
	}

	@Override
	public void setDataDir(File dataDir) {
		throw new UnsupportedOperationException("SPARQL endpoint repository has no data dir");
	}

	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException("SPARQL endpoint repository has no data dir");
	}
	
	@Override
	public boolean isWritable() throws RepositoryException {
		return false;
	}

	@Override
	public ValueFactory getValueFactory() {
		return this.vf;
	}

	@Override
	public void initialize() throws RepositoryException {
		
		if (this.initialized) {
			LOGGER.info("Void repository has already been initialized");
			return;
		}
		
		try {
			this.endpoint = VoidStatistics.getInstance().load(this.voidIRI, this.endpoint);
		} catch (IOException e) {
			throw new RepositoryException("can not read voiD description: " + this.voidIRI + e.getMessage(), e);
		}
		
		this.initialized = true;
	}

	@Override
	public void shutDown() throws RepositoryException {
		// TODO: remove statistics from VOID repository?
	}

	@Override
	public RepositoryConnection getConnection() throws RepositoryException {
		return new VoidRepositoryConnection(this);
	}	

}
