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
package de.uni_koblenz.west.splendid;

//import org.eclipse.rdf4j.model.LiteralFactory;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
//import org.eclipse.rdf4j.model.IRIFactory;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
//import org.eclipse.rdf4j.model.impl.BNodeFactoryImpl;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;
import org.eclipse.rdf4j.query.Update;
//import org.eclipse.rdf4j.result.ContextResult;
//import org.eclipse.rdf4j.result.ModelResult;
//import org.eclipse.rdf4j.result.NamespaceResult;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
//import org.eclipse.rdf4j.store.StoreException;

import de.uni_koblenz.west.splendid.helpers.QueryExecutor;
import de.uni_koblenz.west.splendid.helpers.ReadOnlyRepositoryConnection;

/**
 * RepositoryConnection that communicates with a SPARQL endpoint via HTTP.
 * 
 * @author Olaf Goerlitz
 */
public class VoidRepositoryConnection extends ReadOnlyRepositoryConnection {
	
	protected final String endpoint;
	protected final ValueFactory vf;
	
	/**
	 * Creates a RepositoryConnection for the voiD repository.
	 * 
	 * @param repository the repository which is connected.
	 */
	public VoidRepositoryConnection(VoidRepository repository) {
		super(repository);
		
		this.endpoint = repository.getEndpoint().stringValue();
		
		// reuse repository specific factories for better performance
//		BNodeFactoryImpl bf = new BNodeFactoryImpl();
//		IRIFactory uf = repository.getIRIFactory();
//		LiteralFactory lf = repository.getLiteralFactory();
//		this.vf = new ValueFactoryImpl(bf, uf, lf);
		this.vf = new ValueFactoryImpl();
	}
	
	// -------------------------------------------------------------------------
	
	//Dummy
	@Override
	public void removeWithoutCommit(Resource subject,IRI predicate,Value object,Resource... contexts) throws RepositoryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	//Dummy
	@Override
	public void addWithoutCommit(Resource subject,IRI predicate,Value object,Resource... contexts) throws RepositoryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	//Dummy
	@Override
	public void begin() throws RepositoryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	//Dummy
	@Override
	public boolean isActive() throws RepositoryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String baseIRI)
//			throws StoreException, MalformedQueryException {
			throws RepositoryException, MalformedQueryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String baseIRI)
//			throws StoreException, MalformedQueryException {
			throws RepositoryException, MalformedQueryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Query prepareQuery(QueryLanguage ql, String query, String baseIRI)
//			throws StoreException, MalformedQueryException {
			throws RepositoryException, MalformedQueryException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Prepares a query that produces sets of value tuples.
	 * 
	 * @param ql
	 *        The query language in which the query is formulated.
	 * @param query
	 *        The query string.
	 * @param baseIRI
	 *        The base IRI to resolve any relative IRIs that are in the query
	 *        against, can be <tt>null</tt> if the query does not contain any
	 *        relative IRIs.
	 * @throws IllegalArgumentException
	 *         If the supplied query is not a tuple query.
	 * @throws MalformedQueryException
	 *         If the supplied query is malformed.
	 * @throws UnsupportedQueryLanguageException
	 *         If the supplied query language is not supported.
	 */
	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String baseIRI)
//			throws StoreException, MalformedQueryException {
			throws RepositoryException, MalformedQueryException {
		
		if (ql != QueryLanguage.SPARQL)
			throw new UnsupportedQueryLanguageException("only SPARQL supported");
		if (query == null)
			throw new IllegalArgumentException("query is null");
		if (baseIRI != null)
			throw new IllegalArgumentException("base/relative IRIs not allowed");
		
		return QueryExecutor.prepareTupleQuery(query, this.endpoint, null);
	}

//	public <H extends RDFHandler> H exportMatch(Resource subj, IRI pred, Value obj, boolean includeInferred, H handler, Resource... contexts) throws StoreException, RDFHandlerException {
	public void exportStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
		throw new UnsupportedOperationException("Not yet implemented");
	}
	
	@Override
//	public ContextResult getContextIDs() throws StoreException {
	public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
//	public String getNamespace(String prefix) throws StoreException {
	public String getNamespace(String prefix) throws RepositoryException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
//	public NamespaceResult getNamespaces() throws StoreException {
	public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
		throw new UnsupportedOperationException("Not yet implemented");
	}
	
	@Override
//	public ModelResult match(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws StoreException {
	public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
		throw new UnsupportedOperationException("Not yet implemented");
	}
	
	@Override
	public Update prepareUpdate(QueryLanguage ql, String update, String baseIRI)
			throws RepositoryException, MalformedQueryException {
		throw new UnsupportedOperationException("SPARQL update is not supported");
	}
	
	// Sesame 2 only ===========================================================
	
	@Override
	public long size(Resource... contexts) throws RepositoryException {
		throw new UnsupportedOperationException("Not yet implemented");
	}
	
	// Sesame 3 only ===========================================================
	
	/**
	 * Gets a ValueFactory for this RepositoryConnection.
	 * 
	 * @return A repository-specific ValueFactory.
	 */
	@Override
	public ValueFactory getValueFactory() {
		return this.vf;
	}
	
//	@Override
//	public void begin() throws StoreException {
//		throw new UnsupportedOperationException("not yet implemented");
//	}
//
//	@Override
//	public void close() throws StoreException {
//		throw new UnsupportedOperationException("not yet implemented");
//	}
//
//	@Override
//	public boolean isOpen() throws StoreException {
//		throw new UnsupportedOperationException("not yet implemented");
//	}
//
//	@Override
//	public ModelResult match(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts)
//			throws StoreException {
//		throw new UnsupportedOperationException("not yet implemented");
//	}
//
//	@Override
//	public long sizeMatch(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts)
//			throws StoreException {
//		throw new UnsupportedOperationException("not yet implemented");
//	}

}
