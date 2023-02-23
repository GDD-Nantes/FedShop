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
package de.uni_koblenz.west.splendid.test;

import java.io.IOException;
import java.io.StringReader;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

/**
 * @author Olaf Goerlitz
 */
public class Test {
	
	private static final String DATA = "<http://mpii.de/yago/resource/Barack_Obama> <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n"+
	                                   "<http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> <http://dbpedia.org/resource/Democratic_Party_%28United_States%29>.";
	
	private static final String QUERY = 
			"SELECT ?x ?party  WHERE {"+
            "<http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party ."+
            "?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> ."+
            "}";
	
	private Repository rep;
	
	public Test() {
		rep = new SailRepository(new MemoryStore());
		try {
			rep.initialize();
		} catch (RepositoryException e) {
			throw new RuntimeException("initialization of statistics repository failed", e);
		}
	}
	
	public void test() {
		
		try {
			
			RepositoryConnection con = this.rep.getConnection();
			try {
				con.add(new StringReader(DATA), "http://test.de", RDFFormat.NTRIPLES);
				
				TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, QUERY);
				TupleQueryResult result = query.evaluate();
				while (result.hasNext())
					System.out.println("result: " + result.next());
				
			} catch (QueryEvaluationException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (RDFParseException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				con.close();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new Test().test();
	}

}
