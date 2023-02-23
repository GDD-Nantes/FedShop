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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;

import de.uni_koblenz.west.splendid.VoidRepository;

/**
 * Defines constants for the VoidRepository schema which is used by
 * {@link VoidRepositoryFactory}s to initialize {@link VoidRepository}s.
 * 
 * @author Olaf Goerlitz
 */
public class VoidRepositorySchema {
	
	private static final ValueFactory vf = ValueFactoryImpl.getInstance();

	/**
	 * The VoidRepository schema namespace (
	 * <tt>http://rdfs.org/ns/void#</tt>).
	 */
	public static final String NAMESPACE = "http://rdfs.org/ns/void#";

	/** <tt>http://rdfs.org/ns/void#sparqlEndpoint</tt> */
	public static final IRI ENDPOINT = vf.createIRI(NAMESPACE, "sparqlEndpoint");
	
}
