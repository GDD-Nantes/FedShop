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
package de.uni_koblenz.west.evaluation;

import static org.eclipse.rdf4j.query.QueryLanguage.SPARQL;

import java.io.IOException;
import java.util.Iterator;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.test.config.Configuration;
import de.uni_koblenz.west.splendid.test.config.ConfigurationException;
import de.uni_koblenz.west.splendid.test.config.Query;

/**
 * Evaluation of the query processing.
 * 
 * @author Olaf Goerlitz
 */
public class QueryProcessingEval {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessingEval.class);
	
	private static final String DEFAULT_CONFIG = "setup/fed-test.properties";
	
	public static void main(String[] args) {
		
		String configFile;
		
		// assign configuration file
		if (args.length == 0) {
			configFile = DEFAULT_CONFIG;
			LOGGER.info("using default config: " + configFile);
		} else {
			configFile = args[0];
			LOGGER.info("using config: " + configFile);
		}
		
		// initialize configuration and repository
		Repository repository = null;
		Iterator<Query> queries = null;
		try {
			Configuration config = Configuration.load(configFile);
			repository = config.createRepository();
			queries = config.getQueryIterator();
		} catch (IOException e) {
			LOGGER.error("cannot load test config: " + e.getMessage());
		} catch (ConfigurationException e) {
			LOGGER.error("failed to create repository: " + e.getMessage());
		}
		
		// execute all queries
		while (queries.hasNext()) {
			Query query = queries.next();
			
			LOGGER.info("next Query: " + query.getName());
			
			try {
				RepositoryConnection con = repository.getConnection();
				
				try {
					TupleQuery tupleQuery = con.prepareTupleQuery(SPARQL, query.getQuery());
					
					long start = System.currentTimeMillis();
					TupleQueryResult result = tupleQuery.evaluate();
					LOGGER.info((System.currentTimeMillis() - start)/1000 + " seconds elapsed");
					
					try {
						// count elements of result set
						int count = 0;
						while (result.hasNext()) {
							result.next();
							count++;
						}
						
						LOGGER.info("RESULT SIZE: " + count);
					}
					finally {
						result.close();
					}
					
				} catch (MalformedQueryException e) {
					LOGGER.error("malformed query:\n" + query, e);
					throw new IllegalArgumentException("Malformed query: ", e);
				} catch (QueryEvaluationException e) {
					LOGGER.error("failed to evaluate query on repository: " + repository + "\n" + query, e);
					Throwable cause = e.getCause();
					for (int i = 1; cause != null; cause = cause.getCause(), i++) {
						LOGGER.error("CAUSE " + i + ": " + cause);
					}
				}
			    finally {
			       con.close();
			    }
			} catch (RepositoryException e) {
				LOGGER.error("failed to open/close repository connection", e);
			}
		}
		
		try {
			LOGGER.info("shutting down repository");
			repository.shutDown();
			LOGGER.info("shutdown complete");
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		
	}

}
