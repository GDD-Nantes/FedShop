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
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import de.uni_koblenz.west.splendid.FederationSail;
import de.uni_koblenz.west.splendid.helpers.QueryExecutor;

/**
 * Evaluates the implementation of the query evaluation strategy.
 * 
 * @author Olaf Goerlitz
 */
public class EvaluationStrategyTest {
	
	private static final String N3_DATA1 = 
		"@prefix ex: <http://ex.com/>." + 
		"ex:Alice a ex:Person." +
		"ex:Bob a ex:Person.";
	private static final String N3_DATA2 = 
		"@prefix ex: <http://ex.com/>." + 
		"ex:Alice ex:likes ex:Bob." +
		"ex:Bob ex:likes ex:Banana.";
	
	private static final String PREFIX = "PREFIX ex: <http://ex.com/>\n";
	
	private static final ValueFactory vf = ValueFactoryImpl.getInstance();
	
	private static final URI PERSON = vf.createURI("http://ex.com/Person");
	private static final URI LIKES = vf.createURI("http://ex.com/likes");
	
	private static final URI ALICE = vf.createURI("http://ex.com/Alice");
	private static final URI BOB   = vf.createURI("http://ex.com/Bob");
	
	private Repository[] repositories;
//	private SourceFinder sourceFinder;
	private EvaluationStrategyImpl evalStrategy;
	private FederationSail federationSail;

	private String query;
	private StatementPattern sp;
	private List<BindingSet> bindings;
	
	@Before
//	public void setUp() throws StoreException {
	public void setUp() throws SailException {
		
		repositories = new Repository[] { 
				initMemoryStore(N3_DATA1),
				initMemoryStore(N3_DATA2)
		};
		
//		sourceFinder = new SourceFinder(repositories);
//		evalStrategy = new FederationEvalStrategy(sourceFinder, vf);
		federationSail = new FederationSail();
		federationSail.setMembers(Arrays.asList(repositories));
		federationSail.initialize();
	}
	
	@Test
	public void testStatementPatterns() {
		
		// match { ?s ?p ?o }  :  4 results
		sp = new StatementPattern(new Var("s"), new Var("p"), new Var("o"));
		bindings = QueryExecutor.eval(evalStrategy, sp);
		
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 4);
		
		// match { ?s a ex:Person }  :  2 results
		sp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", PERSON));
		bindings = QueryExecutor.eval(evalStrategy, sp);
		
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 2);
		
		// match { ?s ex:likes ?o }  :  2 results
		sp = new StatementPattern(new Var("s"), new Var("p", LIKES), new Var("o"));
		bindings = QueryExecutor.eval(evalStrategy, sp);
		
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 2);
	}
	
	@Test
	public void testJoins() {
		
		// match { ?s ex:likes ?o. ?o a ex:Person }  :  1 result
		StatementPattern sp1 = new StatementPattern(new Var("o"), new Var("p1", RDF.TYPE), new Var("type", PERSON));
		StatementPattern sp2 = new StatementPattern(new Var("s"), new Var("p2", LIKES), new Var("o"));
		Join join = new Join(sp1, sp2);
		bindings = QueryExecutor.eval(evalStrategy, join);
		
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 1);
		Assert.assertTrue(ALICE.equals(bindings.get(0).getValue("s")));
		Assert.assertTrue(BOB.equals(bindings.get(0).getValue("o")));
		
		// match { ?s a ex:Person; ex:likes ?o. ?o a ex:Person }  :  1 result
		StatementPattern sp3 = new StatementPattern(new Var("s"), new Var("p1", RDF.TYPE), new Var("type1", PERSON));
		StatementPattern sp4 = new StatementPattern(new Var("o"), new Var("p2", RDF.TYPE), new Var("type2", PERSON));
		StatementPattern sp5 = new StatementPattern(new Var("s"), new Var("p3", LIKES), new Var("o"));
		Join join1 = new Join(sp4, sp5);
		Join join2 = new Join(sp3, join1);
		bindings = QueryExecutor.eval(evalStrategy, join2);
		
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 1);
		Assert.assertTrue(ALICE.equals(bindings.get(0).getValue("s")));
		Assert.assertTrue(BOB.equals(bindings.get(0).getValue("o")));
	}
	
	@Test
	public void testSailQueries() {
		query = "SELECT * WHERE { ?s ?p ?o }";
		try {
			bindings = QueryExecutor.eval(federationSail, query, false);
			Assert.fail("Should have raised an UnsupportedOperationException");
		} catch (UnsupportedOperationException e) {
		}
//		Assert.assertNotNull(bindings);
//		Assert.assertEquals(bindings.size(), 4);
		
		query = PREFIX + "SELECT * WHERE { ?s a ex:Person }";
		bindings = QueryExecutor.eval(federationSail, query, false);
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 2);
		
		query = PREFIX + "SELECT * WHERE { ?s ex:likes ?o }";
		bindings = QueryExecutor.eval(federationSail, query, false);
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 2);

		query = PREFIX + "SELECT * WHERE { ?s ex:likes ?o. ?o a ex:Person }";
		bindings = QueryExecutor.eval(federationSail, query, false);
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 1);
		Assert.assertTrue(ALICE.equals(bindings.get(0).getValue("s")));
		Assert.assertTrue(BOB.equals(bindings.get(0).getValue("o")));
		
		query = PREFIX + "SELECT * WHERE { ?s a ex:Person; ex:likes ?o. ?o a ex:Person }";
		bindings = QueryExecutor.eval(federationSail, query, false);
		Assert.assertNotNull(bindings);
		Assert.assertEquals(bindings.size(), 1);
		Assert.assertTrue(ALICE.equals(bindings.get(0).getValue("s")));
		Assert.assertTrue(BOB.equals(bindings.get(0).getValue("o")));
	}

	// -------------------------------------------------------------------------
	
	/**
	 * Initialize a memory store with some RDF data.
	 * 
	 * @param rdfData the initial RDF data.
	 * @return the initialized memory store.
	 */
	private Repository initMemoryStore(String rdfData) {
		StringReader reader = new StringReader(rdfData);
		Repository rep = new SailRepository(new MemoryStore());
		try {
			rep.initialize();
			rep.getConnection().add(reader, "http://ex.com/", RDFFormat.N3);
//		} catch (StoreException e) {
		} catch (RepositoryException e) {
			e.printStackTrace();
			return null;
		} catch (RDFParseException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return rep;
	}
	
}
