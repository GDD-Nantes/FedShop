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

import org.junit.Assert;
import org.junit.Test;

import de.uni_koblenz.west.splendid.helpers.QueryExecutor;

/**
 * Testing public SPARQL endpoints.
 * 
 * @author Olaf Goerlitz
 */
public class SparqlEndpointTest {

	@Test
	public void testSparqlEndpoint() {

		String endpoint = "http://dbpedia.org/sparql";
		String query = "SELECT DISTINCT * WHERE {[] a ?type } LIMIT 10";
		
		int size = QueryExecutor.getSize(QueryExecutor.eval(endpoint, query, null));
		Assert.assertTrue(size == 10);
	}
}
