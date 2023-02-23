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
package de.uni_koblenz.west.splendid.test.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.DynamicModel;
import org.eclipse.rdf4j.model.impl.DynamicModelFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.FederationSail;

/**
 * Configuration object holding all settings for a test scenarios.
 * Provides methods to create the corresponding repository etc.
 * 
 * @author Olaf Goerlitz
 */
public class Configuration {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
	
	private static final String PROP_REP_CONFIG 		= "repository.config";
	private static final String PROP_QUERY_DIR  		= "query.directory";
	private static final String PROP_QUERY_EXT  		= "query.extension";
	private static final String PROP_OUT_FILE   		= "output.file";
	private static final String PROP_SPARQL_ENDPOINT 	= "sparql.endpoint";
	
	private File cfgFile;
	private Properties props = new Properties();
	
	private Repository repository;
	
	/**
	 * Private constructor for reading the configuration settings from a file.
	 *  
	 * @param fileName the file containing the configuration settings.
	 * @throws IOException if an error occurred when reading from the file.
	 */
	private Configuration(String fileName) throws IOException {
		this.cfgFile = new File(fileName).getAbsoluteFile();
		this.props.load(new FileReader(this.cfgFile));
		LOGGER.info("loaded configuration from " + cfgFile);
	}
	
	/**
	 * Load configurations setting from a file.
	 * 
	 * @param configFile the file containing the configuration settings.
	 * @return instantiation of the configuration settings
	 * @throws IOException if an error occurred when reading from the file.
	 */
	public static Configuration load(String configFile) throws IOException {
		return new Configuration(configFile);
	}
	
	// -------------------------------------------------------------------------
	
	public FederationSail getFederationSail() throws ConfigurationException {
		
		if (this.repository == null)
			createRepository();

		return ((FederationSail) ((SailRepository) this.repository).getSail());
	}
	
	/**
	 * Creates a new repository for the supplied configuration.
	 * 
	 * @return the initialized repository.
	 * @throws ConfigurationException if an error occurs during the repository configuration.
	 */
	public Repository createRepository() throws ConfigurationException {
		
		if (this.repository != null)
			throw new IllegalStateException("repository has already been created");
		
		// get repository config file
		String repConfig = props.getProperty(PROP_REP_CONFIG);
		if (repConfig == null) {
			throw new ConfigurationException("missing config file setting '" + PROP_REP_CONFIG + "' in " + cfgFile);
		}
		
		// create repository
    	try {
    		// using configuration directory as base for resolving relative URIs
    		RepositoryConfig repConf = RepositoryConfig.create(loadRDFConfig(repConfig), null);
			repConf.validate();
			RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
			RepositoryRegistry registry = RepositoryRegistry.getInstance();
			RepositoryFactory factory = registry.get(implConf.getType()).get();
			if (factory == null) {
				throw new ConfigurationException("Unsupported repository type: " + implConf.getType() + " in repository config");
			}
			this.repository = factory.getRepository(implConf);
			this.repository.initialize();
			return this.repository;
		} catch (RepositoryConfigException e) {
			throw new ConfigurationException("cannot create repository: " + e.getMessage());
		} catch (RepositoryException e) {
			throw new ConfigurationException("cannot initialize repository: " + e.getMessage());
		}
	}

	/**
	 * Returns an iterator over the specified SPARQL Queries.
	 * 
	 * @return the queries wrapped in an iterator.
	 * @throws ConfigurationException if an error occurs during query reading.
	 */
	public Iterator<Query> getQueryIterator() throws ConfigurationException {
		
		return new Iterator<Query>() {
			
			private List<File> files = getQueryList();

			@Override
			public boolean hasNext() {
				return files.size() != 0;
			}

			@Override
			public Query next() {
				if (files.size() == 0)
					throw new IllegalStateException("no more query files");
				
				File file = files.remove(0);
				try {
					String query = readQuery(file);
					
					if (LOGGER.isTraceEnabled())
						LOGGER.trace(file + ":\n" + query);
					else if (LOGGER.isDebugEnabled())
						LOGGER.debug(file.toString());
					
					// remove extension from query name
					String name = file.getName();
					int pos = name.lastIndexOf(".");
					if (pos > 0)
						name = name.substring(0, pos);
					return new Query(name, query);
				} catch (IOException e) {
					throw new RuntimeException("can not load query " + file, e);
				}
			}

			@Override
			public void remove() {}
		};
	}

	/**
	 * Returns an output stream for result writing.
	 *  
	 * @return the output stream for result writing.
	 * @throws ConfigurationException if an error occurs when opening the output stream.
	 */
	public PrintStream getResultStream() throws ConfigurationException {
		String outfile = props.getProperty(PROP_OUT_FILE);
		if (outfile == null) {
			LOGGER.warn("no output file specified");
			return System.out;
		}
		
		File file = new File(cfgFile.toURI().resolve(outfile)).getAbsoluteFile();
		try {
			return new PrintStream(file);
		} catch (FileNotFoundException e) {
			throw new ConfigurationException("cannot open output file: " + e.getMessage());
		}
	}
	
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the list of matching query files in the specified directory.
	 * 
	 * @return list of query files.
	 * @throws ConfigurationException if an error occurs during query reading.
	 */
    private List<File> getQueryList() throws ConfigurationException {
    	
		String queryDir = props.getProperty(PROP_QUERY_DIR);
		String queryExt = props.getProperty(PROP_QUERY_EXT);
		if (queryDir == null)
			throw new ConfigurationException("missing query dir setting '" + PROP_QUERY_DIR + "' in " + cfgFile);
		if (queryExt == null)
			throw new ConfigurationException("missing query extension setting '" + PROP_QUERY_EXT + "' in " + cfgFile);
		
		// split multiple query dirs
		String[] queryDirs = queryDir.split(";");
		List<File> queries = new ArrayList<File>();
		
		for (String qDir : queryDirs) {
			File dir = new File(cfgFile.toURI().resolve(qDir)).getAbsoluteFile();
			if (!dir.isDirectory() || !dir.canRead())
				LOGGER.warn("cannot read query directory: " + dir);
			
			for (File file : dir.listFiles()) {
				if (file.isFile() && file.getName().endsWith(queryExt)) {
					queries.add(file);
				}
			}
		}
		
		if (queries.size() == 0)
			LOGGER.error("found no matching queries");
		
		Collections.sort(queries);
		return queries;
    }
    
	/**
	 * Reads a SPARQL query from a file.
	 * 
	 * @param file the file to read.
	 * @return the query.
	 * @throws IOException if an error occurs during query reading.
	 */
	private String readQuery(File query) throws IOException {
		StringBuffer buffer = new StringBuffer();
		BufferedReader r = new BufferedReader(new FileReader(query));
		String input;
		while((input = r.readLine()) != null) {
			buffer.append(input).append("\n");
		}
		return buffer.toString();
	}
	
	/**
	 * Loads the repository configuration.
	 * 
	 * @param repConfig the name of the repository configuration file.
	 * @return the repository configuration model.
	 * @throws ConfigurationException if an error occurs while loading the configuration.
	 */
	private Model loadRDFConfig(String repConfig) throws ConfigurationException {
		
		String baseURI = cfgFile.toURI().toString();
		File file = new File(cfgFile.toURI().resolve(repConfig)).getAbsoluteFile();
		RDFFormat format = Rio.getParserFormatForFileName(repConfig).get();
		if (format == null)
			throw new ConfigurationException("unknown RDF format of repository config: " + file);
		
		try {
			Model model = new DynamicModel(new DynamicModelFactory());
			RDFParser parser = Rio.createParser(format);
			parser.setRDFHandler(new StatementCollector(model));
			parser.parse(new FileReader(file), baseURI);
			return model;
			
		} catch (UnsupportedRDFormatException e) {
			throw new ConfigurationException("cannot load repository config, unsupported RDF format (" + format + "): " + file);
		} catch (RDFParseException e) {
			throw new ConfigurationException("cannot load repository config, RDF parser error: " + e.getMessage() + ": " + file);
		} catch (RDFHandlerException e) {
			throw new ConfigurationException("cannot load repository config, RDF handler error: " + e.getMessage() + ": " + file);
		} catch (IOException e) {
			throw new ConfigurationException("cannot load repository config, IO error: " + e.getMessage());
		}
	}

	public String getSparqlEndpoint() {
		return this.props.getProperty(PROP_SPARQL_ENDPOINT);
	}

}
