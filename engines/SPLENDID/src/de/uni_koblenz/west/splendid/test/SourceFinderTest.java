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
package de.uni_koblenz.west.splendid.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.FederationSail;
import de.uni_koblenz.west.splendid.helpers.QueryExecutor;
import de.uni_koblenz.west.splendid.sources.SourceSelector;
import de.uni_koblenz.west.splendid.test.config.Configuration;
import de.uni_koblenz.west.splendid.test.config.ConfigurationException;
import de.uni_koblenz.west.splendid.test.config.Query;

/**
 * 
 * @author Olaf Goerlitz
 */
public class SourceFinderTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SourceFinderTest.class);
	
	private static final String CONFIG_FILE = "setup/life-science-config.prop";
	
	private static Repository REPOSITORY;
	private static Iterator<Query> QUERIES;
	private static SourceSelector finder;
	
	public static void main(String[] args) {
		
		// check arguments for name of configuration file
		String configFile;
		if (args.length == 0) {
			LOGGER.info("no config file specified; using default: " + CONFIG_FILE);
			configFile = CONFIG_FILE;
		} else {
			configFile = args[0];
		}
		
		// load configuration file and create repository
		setup(configFile);
	}
	
	private static void setup(String configFile) {
		
		try {
			Configuration config = Configuration.load(configFile);
			REPOSITORY = config.createRepository();
			QUERIES = config.getQueryIterator();
			// get finder from configuration
			FederationSail fedSail = config.getFederationSail();
			finder = fedSail.getSourceSelector();
//			System.out.println("finder rdf:type=" + finder.isHandleRDFType());
		} catch (IOException e) {
			LOGGER.error("cannot load test config: " + e.getMessage());
		} catch (ConfigurationException e) {
			LOGGER.error("failed to create repository: " + e.getMessage());
		}
	}
	
	// -------------------------------------------------------------------------
	
    @BeforeClass
    public static void setUp() {
    	setup(CONFIG_FILE);
    }
    
	public void testQueries() {
		while (QUERIES.hasNext()) {
			String query = QUERIES.next().getQuery();
			
			long start = System.currentTimeMillis();
			List<BindingSet> result = QueryExecutor.eval(REPOSITORY, query);
			LOGGER.info("Evaluation time: " + (System.currentTimeMillis() - start));
			LOGGER.info("RESULT SIZE: " + (result != null ? result.size() : -1));
		}
	}

}
