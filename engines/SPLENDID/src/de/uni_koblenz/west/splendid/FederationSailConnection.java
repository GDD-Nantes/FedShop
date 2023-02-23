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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.helpers.ReadOnlySailConnection;

/**
 * Wraps multiple remote repositories with SPARQL endpoints into one
 * {@link SailConnection}. Queries are split into fragments and forwarded to
 * endpoints which can probably return results.<br/><br/>
 * 
 * Statistics are used to select suitable remote repositories.
 * {@link TripleSource}s, which are used to evaluate single
 * statement patterns, are not employed.
 * 
 * @author Olaf Goerlitz
 */
@SuppressWarnings({"deprecation","removal"})
public class FederationSailConnection extends ReadOnlySailConnection {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FederationSailConnection.class);

	private final QueryOptimizer optimizer;
	private final EvaluationStrategy strategy;
	
	/**
	 * Create a Sail connection which wraps the members repository connections.
	 * Adopted from <tt>FederationConnection.FederationConnection()</tt>.
	 * 
	 * @param sail the federation Sail.
	 * @param stats the statistics to use.
	 */
	public FederationSailConnection(FederationSail sail) {
		
		super(sail);  // mandatory for Sesame 2, obsolete in Sesame 3
		
		if (sail == null)
			throw new IllegalArgumentException("sail must not be NULL");
		
		this.optimizer = sail.getFederationOptimizer();
		this.strategy = sail.getEvalStrategy();
	}
	
	// -------------------------------------------------------------------------
	
	/**
	 * Evaluates the supplied QueryModel on the federation members.
	 * 
	 * @param query
	 *            The query to evaluate.
	 * @param bindings
	 *            A set of input parameters for the query evaluation. The keys
	 *            reference variable names that should be bound to the value
	 *            they map to.
	 * @param includeInferred
	 *            Indicates whether inferred triples are to be considered in the
	 *            query result. If false, no inferred statements are returned;
	 *            if true, inferred statements are returned if available
	 * @return The result Cursor.
	 * @throws StoreException
	 *             If the Sail encountered an error or invalid internal state.
	 */
	public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr query, Dataset dataset,
			BindingSet bindings, boolean includeInferred) throws SailException {	// Sesame 2:
		
		QueryOptimizerList optimizerList = new QueryOptimizerList();
		
		LOGGER.trace("Incoming query model:\n{}", OperatorTreePrinter.print(query));
		
		// Clone the tuple expression to allow for more aggressive optimizations
		query = query.clone();

		optimizerList.add(new BindingAssigner());
		optimizerList.add(new CompareOptimizer());
		optimizerList.add(new ConjunctiveConstraintSplitter());
		optimizerList.add(new DisjunctiveConstraintOptimizer());
		optimizerList.add(new SameTermFilterOptimizer());
//		optimizerList.add(new FilterOptimizer());
//		optimizerList.add(new QueryModelPruner());
		optimizerList.add(this.optimizer);

		optimizerList.optimize(query, dataset, bindings);
		
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Optimized query model:\n{}", OperatorTreePrinter.print(query));
		
		try {
			return strategy.evaluate(query, EmptyBindingSet.getInstance());
		} catch (QueryEvaluationException e) {
			throw new SailException("query evaluation failed", e);
		}
	}
	
	// Sesame 2: Overriding internal methods ==================================
	
	protected void closeInternal() throws SailException {
		// do nothing, calling super.close() creates a loop in Sesame 2
	}

	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
		throw new UnsupportedOperationException("Not implemented");
	}

	protected String getNamespaceInternal(String prefix) throws SailException {
		throw new UnsupportedOperationException("Not implemented");
	}

	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
		throw new UnsupportedOperationException("Not implemented");
	}

	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("Not implemented");
	}

	protected long sizeInternal(Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("Not implemented");
	}
	
}
