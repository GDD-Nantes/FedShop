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
package de.uni_koblenz.west.splendid.statistics;

import static org.eclipse.rdf4j.query.QueryLanguage.SPARQL;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.vocabulary.VOID2;

/**
 * voiD-based statistics implementation.
 * 
 * @author Olaf Goerlitz
 */
public class VoidStatistics implements RDFStatistics {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VoidStatistics.class);
	
	private static final String USER_DIR = System.getProperty("user.dir") + File.separator;
	
	private static final String VOID_PREFIX = "PREFIX void: <" + VOID2.NAMESPACE + ">\n";
	
	private static final String VAR_GRAPH = "$GRAPH$";
	private static final String VAR_TYPE  = "$TYPE$";
	private static final String VAR_PRED  = "$PRED$";

	private static final String PRED_SOURCE = VOID_PREFIX +
			"SELECT ?source WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint ?source ;" +
			"     void:propertyPartition ?part ." +
			"  ?part void:property <" + VAR_PRED + "> ." +
			"}";
	
	private static final String TYPE_SOURCE = VOID_PREFIX +
			"SELECT ?source WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint ?source ;" +
			"     void:classPartition ?part ." +
			"  ?part void:class <" + VAR_TYPE + ">" +
			"}";
	
	private static final String TRIPLE_COUNT = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:triples ?count ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ." +
			"}";
	
	private static final String DISTINCT_PREDICATES = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:properties ?count ." +
			"}";

	private static final String DISTINCT_SUBJECTS = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:distinctSubjects ?count ." +
			"}";

	private static final String DISTINCT_PRED_SUBJECTS = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:propertyPartition ?part ." +
			"  ?part void:property <" + VAR_PRED + "> ;" +
			"        void:distinctSubjects ?count ." +
			"}";
	
	private static final String DISTINCT_OBJECTS = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:distinctObjects ?count ." +
			"}";
	
	private static final String DISTINCT_PRED_OBJECTS = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:propertyPartition ?part ." +
			"  ?part void:property <" + VAR_PRED + "> ;" +
			"        void:distinctObjects ?count ." +
			"}";
	
	private static final String TYPE_TRIPLES = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:classPartition ?part ." +
			"  ?part void:class <" + VAR_TYPE + "> ;" +
			"        void:entities ?count ." +
			"}";
	
	private static final String PRED_TRIPLES = VOID_PREFIX +
			"SELECT ?count WHERE {" +
			"  [] a void:Dataset ;" +
			"     void:sparqlEndpoint <" + VAR_GRAPH + "> ;" +
			"     void:propertyPartition ?part ." +
			"  ?part void:property <" + VAR_PRED + "> ;" +
			"        void:triples ?count ." +
			"}";
	
	private static final ValueFactory uf = SimpleValueFactory.getInstance();
	private static final IRI DATASET = uf.createIRI(VOID2.Dataset.toString());
	private static final IRI ENDPOINT = uf.createIRI(VOID2.sparqlEndpoint.toString());
	
	protected static final VoidStatistics singleton = new VoidStatistics();
	
	private final Repository voidRepository;
	
	// --- STATIC -------------------------------------------------------------

	public static VoidStatistics getInstance() {
		return singleton;
	}
	
	// --- PRIVATE ------------------------------------------------------------
	
	/**
	 * Private constructor used for singleton creation.
	 */
	private VoidStatistics() {
		this.voidRepository = new SailRepository(new MemoryStore());
		try {
			this.voidRepository.init();
		} catch (RepositoryException e) {
			throw new RuntimeException("initialization of statistics repository failed", e);
		}
	}
	
	/**
	 * Returns the count value defined by the supplied query and variable substitutions.
	 * 
	 * @param query the query to be executed on the voiD repository.
	 * @param vars the variable bindings to be substituted in the query.
	 * @return the resulting count value.
	 */
	private long getCount(String query, String... vars) {
		
		// replace query variables
		for (int i = 0; i < vars.length; i++) {
			query = query.replace(vars[i], vars[++i]);
		}
		
		List<String> bindings = evalQuery(query, "count");
		
		// check result validity
		if (bindings.size() == 0) {
			LOGGER.warn("found no count for " + Arrays.asList(vars));
			return -1;
		}
		if (bindings.size() > 1)
			LOGGER.warn("found multiple counts for " + Arrays.asList(vars));
		
		return Long.parseLong(bindings.get(0));
	}
	
	/**
	 * Evaluates the given query and returns the result values for the specified binding name.
	 * 
	 * @param query the query to evaluate.
	 * @param bindingName the desired result bindings.
	 * @return a list of result binding values.
	 */
	private List<String> evalQuery(String query, String bindingName) {
		try {
			RepositoryConnection con = this.voidRepository.getConnection();
			try {
				TupleQuery tupleQuery = con.prepareTupleQuery(SPARQL, query);
				TupleQueryResult result = tupleQuery.evaluate();
				
				try {
					List<String> bindings = new ArrayList<String>();
					while (result.hasNext()) {
						bindings.add(result.next().getValue(bindingName).stringValue());
					}
					return bindings;
				} catch (QueryEvaluationException e) {
					LOGGER.error("failed to handle query result from voiD repository, " + e.getMessage() + "\n" + query, e);
				} finally {
					result.close();
				}
			} catch (IllegalArgumentException e) {
				LOGGER.error("not a tuple query, " + e.getMessage() + "\n" + query, e);	
			} catch (RepositoryException e) {
				LOGGER.error("failed to create tuple query, " + e.getMessage() + "\n" + query, e);
			} catch (MalformedQueryException e) {
				LOGGER.error("malformed query, " + e.getMessage() + "\n" + query, e);
			} catch (QueryEvaluationException e) {
				LOGGER.error("failed to evaluate query on voiD repository, " + e.getMessage() + "\n" + query, e);
			} finally {
				con.close();
			}
		} catch (RepositoryException e) {
			LOGGER.error("failed to open/close voiD repository connection, " + e.getMessage() + "\n" + query, e);
		}
		return null;
	}
	
	private List<IRI> getEndpoints(IRI voidIRI, RepositoryConnection con) throws RepositoryException {
		ValueFactory uf = this.voidRepository.getValueFactory();
		IRI voidDataset = uf.createIRI(VOID2.Dataset.toString());
		IRI sparqlEndpoint = uf.createIRI(VOID2.sparqlEndpoint.toString());
		List<IRI> endpoints = new ArrayList<IRI>();
		
		for (Statement dataset : con.getStatements(null, RDF.TYPE, voidDataset, false, voidIRI).asList()) {
			for (Statement endpoint : con.getStatements(dataset.getSubject(), sparqlEndpoint, null, false, voidIRI).asList()) {
				// TODO: endpoint may be a literal
				endpoints.add((IRI) endpoint.getObject());
			}
		}
		return endpoints;
	}
	
	// -------------------------------------------------------------------------
	
	@Override
	public Set<Graph> findSources(String sValue, String pValue, String oValue, boolean handleType) {
		
		Set<Graph> sources = new HashSet<Graph>();
		
		if (pValue == null) {
			LOGGER.info("found triple pattern with unbound predicate: selecting all sources");
			sources.addAll(getEndpoints());
			return sources;
		}
		
		String query = null;
		// query for RDF type occurrence if rdf:type with bound object is used
		if (handleType && RDF.TYPE.stringValue().equals(pValue) && oValue != null) {
			query = TYPE_SOURCE.replace(VAR_TYPE, oValue);
		} else { // else query for predicate occurrence
			query = PRED_SOURCE.replace(VAR_PRED, pValue);
		}
		
		// execute query and get all source bindings
		for (String graph : evalQuery(query, "source")) {
			sources.add(new Graph(graph));
		}
		return sources;
	}
	
	@Override
	public long getTripleCount(Graph g) {
		return getCount(TRIPLE_COUNT, VAR_GRAPH, g.toString());
	}
	
	@Override
	public long getPredicateCount(Graph g, String predicate) {
		return getCount(PRED_TRIPLES, VAR_GRAPH, g.toString(), VAR_PRED, predicate);
	}
	
	@Override
	public long getTypeCount(Graph g, String type) {
		return getCount(TYPE_TRIPLES, VAR_GRAPH, g.toString(), VAR_TYPE, type);
	}
	
	@Override
	public long getDistinctPredicates(Graph g) {
		return getCount(DISTINCT_PREDICATES, VAR_GRAPH, g.toString());
	}
	
	@Override
	public long getDistinctSubjects(Graph g) {
		return getCount(DISTINCT_SUBJECTS, VAR_GRAPH, g.toString());
	}
	
	@Override
	public long getDistinctSubjects(Graph g, String predicate) {
		return getCount(DISTINCT_PRED_SUBJECTS, VAR_GRAPH, g.toString(), VAR_PRED, predicate);
	}
	
	@Override
	public long getDistinctObjects(Graph g) {
		return getCount(DISTINCT_OBJECTS, VAR_GRAPH, g.toString());
	}

	@Override
	public long getDistinctObjects(Graph g, String predicate) {
		return getCount(DISTINCT_PRED_OBJECTS, VAR_GRAPH, g.toString(), VAR_PRED, predicate);
	}
	
	// -------------------------------------------------------------------------
	
	/**
	 * Extracts all SPARQL endpoints of the datasets.
	 * 
	 * @return the list of SPARQL endpoints.
	 */
	public List<Graph> getEndpoints() {
		
		List<Graph> sources = new ArrayList<Graph>();
		
		ValueFactory uf = this.voidRepository.getValueFactory();
		IRI voidDataset = uf.createIRI(VOID2.Dataset.toString());
		IRI sparqlEndpoint = uf.createIRI(VOID2.sparqlEndpoint.toString());
		
		try {
			RepositoryConnection con = this.voidRepository.getConnection();
			
			try {
				for (Statement dataset : con.getStatements(null, RDF.TYPE, voidDataset, false).asList()) {
					for (Statement endpoint : con.getStatements(dataset.getSubject(), sparqlEndpoint, null, false).asList()) {
						sources.add(new Graph(endpoint.getObject().stringValue()));
					}
				}
			} catch (RepositoryException e) {
				e.printStackTrace();
			} finally {
				con.close();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		
		return sources;
	}
	
	/**
	 * Loads the supplied voiD description into the statistics repository.
	 * 
	 * @param voidIRI the IRI of the voiD description to load.
	 * @return the assigned SPARQL endpoint.
	 */
	public IRI load(IRI voidIRI, IRI endpoint) throws IOException {
		if (voidIRI == null)
			throw new IllegalArgumentException("voiD IRI must not be null.");
		
		// initialize parser
		RDFFormat format = Rio.getParserFormatForFileName(voidIRI.stringValue()).get();
		if (format == null) {
			throw new IOException("Unsupported RDF format: " + voidIRI);
		}

		URL voidURL = new URL(voidIRI.stringValue());  // throws IOException
		InputStream in = voidURL.openStream();
		try {
			
			RepositoryConnection con = this.voidRepository.getConnection();
			try {
				
				// check if voiD description has already been loaded
				List<IRI> endpoints = getEndpoints(voidIRI, con);
				if (endpoints.size() > 0) {
					LOGGER.warn("VOID has already been loaded: " + voidIRI);
					return endpoints.get(0);
				}
				
				// add voiD file content to repository
				try {
					con.add(in, voidIRI.stringValue(), format, voidIRI);
				} catch (RDFParseException e) {
					LOGGER.error("can not parse VOID file " + voidIRI + ": " + e.getMessage());
					return null;
				} catch (RepositoryException e) {
					LOGGER.error("can not add VOID file: " + voidIRI, e);
					return null;
				}
				
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("loaded VOID: " + voidURL.getPath().replace(USER_DIR, ""));
				
				if (endpoint == null) {
				
					// check if this voiD description has a valid SPARQL endpoint
					endpoints = getEndpoints(voidIRI, con);

					if (endpoints.size() == 0)
						LOGGER.debug("found no SPARQL endpoint in voiD file");
					if (endpoints.size() > 1)
						// TODO: don't throw Exception but use first endpoint only
						throw new IllegalStateException("found multiple SPARQL endpoints in voiD file");

				return endpoints.iterator().next();
				} else {
					// find dataset resource in specified context
					RepositoryResult<Statement> result = con.getStatements(null, RDF.TYPE, DATASET, false, voidIRI);
					Resource dataset = result.next().getSubject();
					
					// TODO: check that there is only one dataset defined
					
					// remove current SPARQL endpoint and add new one
					con.remove(dataset, ENDPOINT, null, voidIRI);
					con.add(dataset, ENDPOINT, endpoint, voidIRI);
					
					LOGGER.info("set SPARQL endpoint '" + endpoint + "' for " + voidURL.getPath().replace(USER_DIR, ""));
					
					return endpoint;
				}
				
			} catch (RepositoryException e) {
				e.printStackTrace();
//			} catch (RDFParseException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
			} finally {
				con.close();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		} finally {
			in.close();
		}
		
		return null;
	}
	
}
