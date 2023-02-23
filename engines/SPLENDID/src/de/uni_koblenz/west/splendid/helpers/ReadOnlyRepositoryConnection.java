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
package de.uni_koblenz.west.splendid.helpers;

//import org.eclipse.rdf4j.cursor.Cursor;
//import org.eclipse.rdf4j.store.Isolation;
//import org.eclipse.rdf4j.store.StoreException;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryReadOnlyException;
import org.eclipse.rdf4j.repository.base.RepositoryConnectionBase;

import de.uni_koblenz.west.splendid.VoidRepository;

/**
 * Prevents data updates by overriding all modifying methods
 * and throwing {@link RepositoryReadOnlyException}s.
 * 
 * @author Olaf Goerlitz
 */
public abstract class ReadOnlyRepositoryConnection extends RepositoryConnectionBase {
	
	/**
	 * Creates a new read-only RepositoryConnection.
	 * 
	 * @param repository the repository which is connected.
	 */
	public ReadOnlyRepositoryConnection(VoidRepository repository) {
		super(repository);
		if (repository == null)
			throw new IllegalArgumentException("repository must not be NULL");
	}
	
	@Override
//	public void clearNamespaces() throws StoreException {  // Sesame 3
	public void clearNamespaces() throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
	@Override
//	public void commit() throws StoreException {  // Sesame 3
	public void commit() throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
	@Override
//	public void rollback() throws StoreException {  // Sesame 3
	public void rollback() throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
	@Override
//	public void removeNamespace(String prefix) throws StoreException {  // Sesame 3
	public void removeNamespace(String prefix) throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
	@Override
//	public void setNamespace(String prefix, String name) throws StoreException {  // Sesame 3
	public void setNamespace(String prefix, String name) throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
//	public void add(Resource subject, URI predicate, Value object, Resource... contexts) throws StoreException {  // Sesame 3
	protected void addWithoutCommit(Resource subject, URI predicate, Value object, Resource... contexts) throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
//	public void removeMatch(Resource subject, URI predicate, Value object, Resource... contexts) throws StoreException {  // Sesame 3
	protected void removeWithoutCommit(Resource subject, URI predicate, Value object, Resource... contexts) throws RepositoryException {  // Sesame 2
		throw new RepositoryReadOnlyException();
	}
	
	// Sesame 3 only:
	
//	// OVERRIDE: RepositoryConnection ------------------------------------------
//
//	@Override
//	public Isolation getTransactionIsolation() throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//	
//	@Override
//	public boolean isAutoCommit() throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//	
//	@Override
//	public boolean isReadOnly() throws StoreException {
//		return true;
//	}
//	
//	@Override
//	public void removeMatch(Resource subject, URI predicate, Value object,
//			Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void setReadOnly(boolean readOnly) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void setTransactionIsolation(Isolation isolation)
//			throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//	
//	// OVERRIDE: RepositoryConnectionBase --------------------------------------
//	
//	@Override
//	public void add(Cursor<? extends Statement> statementIter,
//			Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(File file, String baseURI, RDFFormat dataFormat,
//			Resource... contexts) throws IOException, RDFParseException,
//			StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(InputStream in, String baseURI, RDFFormat dataFormat,
//			Resource... contexts) throws IOException, RDFParseException,
//			StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(Iterable<? extends Statement> statements,
//			Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(Reader reader, String baseURI, RDFFormat dataFormat,
//			Resource... contexts) throws IOException, RDFParseException,
//			StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(Statement st, Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void add(URL url, String baseURI, RDFFormat dataFormat,
//			Resource... contexts) throws IOException, RDFParseException,
//			StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void clear(Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void remove(Cursor<? extends Statement> statementIter,
//			Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void remove(Iterable<? extends Statement> statements,
//			Resource... contexts) throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
//
//	@Override
//	public void remove(Statement st, Resource... contexts)
//			throws StoreException {
//		throw new UnsupportedOperationException("repository is READ ONLY");
//	}
	
}
