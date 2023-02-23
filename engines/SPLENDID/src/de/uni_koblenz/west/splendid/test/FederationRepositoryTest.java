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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.QueryExecutor;
import de.uni_koblenz.west.splendid.test.config.Configuration;
import de.uni_koblenz.west.splendid.test.config.ConfigurationException;
import de.uni_koblenz.west.splendid.test.config.Query;

/**
 * Test Federation repository which is based on the FederationSail.
 * The federation settings are loaded from a local configuration file.
 * 
 * @author Olaf Goerlitz
 */
public class FederationRepositoryTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FederationRepositoryTest.class);
	
	private static final String CONFIG = "eval/federation-test.properties";
	
	private static Repository REPOSITORY;
	private static Iterator<Query> QUERIES;
	private static String configFile;
	
	private String query;
	
	public static void main(String[] args) {
		if (args.length == 0) {
			String className = FederationRepositoryTest.class.getName();
			System.out.println("USAGE: java " + className + " <CONFIG_FILE>");
			System.exit(1);
		}
		configFile = args[0];
		setUp();
		new FederationRepositoryTest().testQueries();
	}
	
    @BeforeClass
    public static void setUp() {
		try {
			if (configFile == null)
				configFile = CONFIG;
			Configuration config = Configuration.load(configFile);
			REPOSITORY = config.createRepository();
			QUERIES = config.getQueryIterator();
		} catch (IOException e) {
			LOGGER.error("cannot load test config: " + e.getMessage());
		} catch (ConfigurationException e) {
			LOGGER.error("failed to create repository: " + e.getMessage());
		}
    }
    
//	@Test
	public void testPatternQueries() {
		
		query = "SELECT DISTINCT * WHERE { [] a ?type }";
		QueryExecutor.eval(REPOSITORY, query);
		
		query = "SELECT DISTINCT * WHERE { ?x a [] }";
		QueryExecutor.eval(REPOSITORY, query);
		
		// MUST FAIL: unbound predicate
		query = "SELECT DISTINCT * WHERE { [] ?p [] }";
//		try {
			QueryExecutor.eval(REPOSITORY, query);
//			Assert.fail("Should have raised an UnsupportedOperationException");
//		} catch (UnsupportedOperationException e) {
//		}

		// MUST FAIL: unbound predicate
		query = "SELECT DISTINCT ?p WHERE { ?x a ?type; ?p [] }";
		try {
			QueryExecutor.eval(REPOSITORY, query);
			Assert.fail("Should have raised an UnsupportedOperationException");
		} catch (UnsupportedOperationException e) {
		}
		
		// MUST FAIL: join over blank nodes is not supported
		query = "SELECT DISTINCT * WHERE { [] a ?type; ?p [] }";
		try {
			QueryExecutor.eval(REPOSITORY, query);
			Assert.fail("Should have raised an UnsupportedOperationException");
		} catch (UnsupportedOperationException e) {
		}
	}
	
	@Test
	public void testQueries() {
		while (QUERIES.hasNext()) {
			Query query = QUERIES.next();
			
			LOGGER.info("next Query: " + query.getName());
			
			long start = System.currentTimeMillis();
			List<BindingSet> result = QueryExecutor.eval(REPOSITORY, query.getQuery());
//			LOGGER.info("Evaluation time: " + (System.currentTimeMillis() - start));
			LOGGER.info((System.currentTimeMillis() - start)/1000 + " seconds elapsed");
			LOGGER.info("RESULT SIZE: " + (result != null ? result.size() : -1));
		}
	}
	
}
