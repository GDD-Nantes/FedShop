/*
 * This file is part of RDF Federator.
 * Copyright 2010 Olaf Goerlitz
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
package de.uni_koblenz.west.splendid.config;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
//import org.eclipse.rdf4j.store.StoreConfigException;

import de.uni_koblenz.west.splendid.VoidRepository;

/**
 * A {@link RepositoryFactory} that creates {@link VoidRepository}s
 * based on the supplied configuration data.
 * 
 * ATTENTION: This factory must be published with full package name in
 *            META-INF/services/org.eclipse.rdf4j.repository.config.RepositoryFactory
 * 
 * @author Olaf Goerlitz
 */
public class VoidRepositoryFactory implements RepositoryFactory {

	/**
	 * The type of repositories that are created by this factory.
	 * 
	 * @see RepositoryFactory#getRepositoryType()
	 */
	public static final String REPOSITORY_TYPE = "west:VoidRepository";

	/**
	 * Returns the repository's type: <tt>west:VoidRepository</tt>.
	 */
	public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

	/**
	 * Provides a repository configuration object for the configuration data.
	 * 
	 * @return a {@link VoidRepositoryConfig}.
	 */
	public RepositoryImplConfig getConfig() {
		return new VoidRepositoryConfig();
	}

	/**
	 * Returns a Repository instance that has been initialized using the
	 * supplied configuration data.
	 * 
	 * @param config
	 *            the repository configuration.
	 * @return The created (but un-initialized) repository.
	 * @throws StoreConfigException
	 *             If no repository could be created due to invalid or
	 *             incomplete configuration data.
	 */
	@Override
	public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
		
		if (!REPOSITORY_TYPE.equals(config.getType())) {
			throw new RepositoryConfigException("Invalid repository type: " + config.getType());
		}
		assert config instanceof VoidRepositoryConfig;
		VoidRepositoryConfig repConfig = (VoidRepositoryConfig) config;
		
//		return new VoidRepository(repConfig.getVoidURI(), repConfig.getEndpoint());
		return new VoidRepository(repConfig);
	}

}
