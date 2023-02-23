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
package de.uni_koblenz.west.splendid.evaluation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.DistinctIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.UnionIteration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//import org.eclipse.rdf4j.cursor.Cursor;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
//import org.eclipse.rdf4j.query.algebra.evaluation.cursors.DistinctCursor;
//import org.eclipse.rdf4j.query.algebra.evaluation.cursors.UnionCursor;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.JoinIterator;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;
//import org.eclipse.rdf4j.store.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.helpers.QueryExecutor;
import de.uni_koblenz.west.splendid.helpers.SparqlPrinter;
import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.model.BindJoin;
import de.uni_koblenz.west.splendid.model.HashJoin;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.model.RemoteQuery;

/**
 * Implementation of the evaluation strategy for querying distributed data
 * sources. This strategy prefers parallel execution of query operators.
 * 
 * Sesame's {@link TripleSource}s are not applicable, since
 * they only allow for matching single statement patterns. Hence, a dummy
 * {@link TripleSource} is provided to the {@link EvaluationStrategyImpl}
 * which demands it in the constructor (in order to have access to the
 * {@link ValueFactory}). 
 * 
 * @author Olaf Goerlitz
 */
public class FederationEvalStrategy extends EvaluationStrategyImpl {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FederationEvalStrategy.class);
	
	private static final ExecutorService executor = Executors.newCachedThreadPool();
	
	private static final boolean MULTI_THREADED = true;
	private static final boolean COLLECT_BGP_PATTERNS = true;
	
	/**
	 * Creates a new Evaluation strategy using the supplied source finder.
	 * 
	 * @param finder the source finder to use.
	 * @param vf the value factory to use.
	 */
	public FederationEvalStrategy(final ValueFactory vf) {
	
		// use a dummy triple source
		// it can handle only single triple patterns but no basic graph patterns
		super(new TripleSource() {
			@Override public ValueFactory getValueFactory() {
				return vf;
			}
//			@Override public Cursor<? extends Statement> getStatements(
//					Resource subj, IRI pred, Value obj, Resource... contexts) throws StoreException {
			@Override public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
					Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
				throw new UnsupportedOperationException("Statement retrieval is not supported in federation");
			}
		},null,null);
	}
	
	// -------------------------------------------------------------------------
	
//	/**
//	 * Evaluates the left join with the specified set of variable bindings as input.
//	 * IMPORTANT: left joins (optional parts) are currently not evaluated.
//	 * 
//	 * @param leftJoin
//	 *        The Left Join to evaluate
//	 * @param bindings
//	 *        The variables bindings to use for evaluating the expression, if
//	 *        applicable.
//	 * @return A cursor over the variable binding sets that match the join.
//	 */
//	@Override
//	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
//			LeftJoin leftJoin, BindingSet bindings)
//			throws QueryEvaluationException {
//		
//		CloseableIteration<BindingSet, QueryEvaluationException> result;
//		return super.evaluate(leftJoin.getLeftArg(), bindings);
//	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			BindJoin join, BindingSet bindings) throws QueryEvaluationException {
		return new JoinIterator(this, join, bindings);
//		throw new UnsupportedOperationException("bind join not supported");
	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			HashJoin join, BindingSet bindings) throws QueryEvaluationException {
		// eval query if all sub operators are applied on same source
		// TODO optimize with caching
		Set<Graph> sources = new SourceCollector().getSources(join);
		if (COLLECT_BGP_PATTERNS && sources.size() == 1)
			return sendSparqlQuery(join, sources, bindings);
	
//		assert join.getNumberOfArguments() > 0;
		
		// TODO: support different join strategies

		Set<String> resultVars = null;
		CloseableIteration<BindingSet, QueryEvaluationException> joinCursor = null;
		
		TupleExpr[] joinArgs = {join.getLeftArg(), join.getRightArg()};
		
		for (TupleExpr joinArg : joinArgs) {
			
			CloseableIteration<BindingSet, QueryEvaluationException> argCursor;
//			if (MULTI_THREADED) {
//				argCursor = fetchArgResults(joinArg, bindings); 
//			} else {
				argCursor = evaluate(joinArg, bindings);
//			}
			
			
			// init binding names if this is the first argument for the join
			if (joinCursor == null) {
				joinCursor = argCursor;
				resultVars = joinArg.getBindingNames();
				// TODO: can constants vars be removed here?
				if (LOGGER.isTraceEnabled())
					LOGGER.trace("pattern bindings: " + resultVars);
				continue;
			}
			
			// else create hash join (with left and right cursor and join vars)
			Set<String> joinVars = new HashSet<String>(resultVars);
			joinVars.retainAll(joinArg.getBindingNames());
			
			// check for B-Node joins
			for (String varName : joinVars) {
				if (varName.startsWith("-anon"))
					throw new UnsupportedOperationException("blank node joins are not supported");
			}
			
//			if (joinVars.size() == 0) {
//				for (TupleExpr arg : join.getArgs()) {
//					LOGGER.info("cross-prod ARG: " + OperatorTreePrinter.print(arg));					
//				}
//			}
//			
			joinCursor = new HashJoinCursor(joinCursor, argCursor, joinVars);
			resultVars.addAll(joinArg.getBindingNames());

			// TODO: can constants vars be removed here?
			if (LOGGER.isTraceEnabled())
				LOGGER.trace("argument bindings: " + joinVars + "; join bindings: " + resultVars);
		}

		return joinCursor;
	}

	/**
	 * Evaluates the join with the specified set of variable bindings as input.
	 * 
	 * @param join
	 *        The Join to evaluate
	 * @param bindings
	 *        The variables bindings to use for evaluating the expression, if
	 *        applicable.
	 * @return A cursor over the variable binding sets that match the join.
	 */
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			Join join, BindingSet bindings) throws QueryEvaluationException {
		
		if (join instanceof BindJoin) {
			return evaluate((BindJoin) join, bindings);
		}
		if (join instanceof HashJoin) {
			return evaluate((HashJoin) join, bindings);
		}
		
		throw new IllegalArgumentException("join type not supported: " + join);
	}

	/**
	 * Evaluates the statement pattern against the supplied sources with the
	 * specified set of variable bindings as input.
	 * 
	 * @param sp
	 *        The Statement Pattern to evaluate
	 * @param bindings
	 *        The variables bindings to use for evaluating the expression, if
	 *        applicable.
	 * @return A cursor over the variable binding sets that match the statement
	 *         pattern.
	 */
	@Override
//	public Cursor<BindingSet> evaluate(StatementPattern sp, BindingSet bindings) throws StoreException {
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern sp, BindingSet bindings) throws QueryEvaluationException {

		if (sp instanceof MappedStatementPattern) {
//			Set<Graph> sources = graphMap.get(sp);
			Set<Graph> sources = ((MappedStatementPattern) sp).getSources();
			
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("EVAL PATTERN {" + OperatorTreePrinter.print(sp) + "} on sources " + sources);
			return sendSparqlQuery(sp, sources , bindings);
		}
		throw new IllegalArgumentException("pattern has no sources");

	}
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			UnaryTupleOperator expr, BindingSet bindings)
			throws QueryEvaluationException {
		if (expr instanceof RemoteQuery) {
			return this.evaluate((RemoteQuery) expr, bindings);
		} else {
			return super.evaluate(expr, bindings);
		}
	}
	
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(RemoteQuery query, BindingSet bindings) throws QueryEvaluationException {
		// evaluate query on SPARQL endpoint
		// 1. pattern group on single source OR
		// 2. single pattern on multiple sources
//		return this.evaluate(query.getArg(), bindings);
		return this.sendSparqlQuery(query.getArg(), query.getSources(), bindings);
	}
	
	// -------------------------------------------------------------------------
	
	private CloseableIteration<BindingSet, QueryEvaluationException> sendSparqlQuery(TupleExpr expr, Set<Graph> sources, BindingSet bindings) {
		
		// check if there are any sources to query
		if (sources.size() == 0) {
			LOGGER.warn("Cannot find any source for: " + OperatorTreePrinter.print(expr));
			return new EmptyIteration<BindingSet, QueryEvaluationException>();
		}
		
//		if (expr instanceof StatementPattern)
//			LOGGER.error("is statement pattern");
		
		// TODO: need to know actual projection and join variables to reduce transmitted data
		
		CloseableIteration<BindingSet, QueryEvaluationException> cursor;
		List<CloseableIteration<BindingSet, QueryEvaluationException>> cursors = new ArrayList<CloseableIteration<BindingSet, QueryEvaluationException>>(sources.size());
		final String query = "SELECT REDUCED * WHERE {" + SparqlPrinter.print(expr) + "}";
		
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Sending SPARQL query to '" + sources + " with bindings " + bindings + "\n" + query);
		
		for (final Graph rep : sources) {
			if (MULTI_THREADED)
				cursors.add(getMultiThread(rep, query, bindings));
			else
				cursors.add(QueryExecutor.eval(rep.toString(), query, bindings));
		}
		

		// create union if multiple sources are involved
		if (cursors.size() > 1) {
//			cursor = new UnionCursor<BindingSet>(cursors);
			cursor = new UnionIteration<BindingSet, QueryEvaluationException>(cursors);
		} else {
			cursor = cursors.get(0);
		}

		// Filter any duplicates
//		cursor = new DistinctCursor<BindingSet>(cursor);
		// TODO: check if this is bad for performance
		cursor = new DistinctIteration<BindingSet, QueryEvaluationException>(cursor);

		return cursor;
		
	}
	
//	public Cursor<BindingSet> getMultiThread(final Graph source, final String query) {
	public CloseableIteration<BindingSet, QueryEvaluationException> getMultiThread(final Graph source, final String query, final BindingSet bindings) {
//		Callable<Cursor<BindingSet>> callable = new Callable<Cursor<BindingSet>>() {
		Callable<CloseableIteration<BindingSet, QueryEvaluationException>>  callable = new Callable<CloseableIteration<BindingSet, QueryEvaluationException>>() {
//			@Override public Cursor<BindingSet> call() {
			@Override public CloseableIteration<BindingSet, QueryEvaluationException> call() {
//				return QueryExecutor.evalQuery(repository, query);
				return QueryExecutor.eval(source.toString(), query, bindings);
			}
		};
//		Future<Cursor<BindingSet>> future = executor.submit(callable);
		Future<CloseableIteration<BindingSet, QueryEvaluationException>> future = executor.submit(callable);
		return new AsyncCursor<BindingSet>(future);
	}	
	
//	public Cursor<BindingSet> fetchArgResults(final TupleExpr joinArg, final BindingSet bindings) {
	public CloseableIteration<BindingSet, QueryEvaluationException>  fetchArgResults(final TupleExpr joinArg, final BindingSet bindings) {
//		Callable<Cursor<BindingSet>> callable = new Callable<Cursor<BindingSet>>() {
		Callable<CloseableIteration<BindingSet, QueryEvaluationException>>  callable = new Callable<CloseableIteration<BindingSet, QueryEvaluationException>>() {
//			@Override public Cursor<BindingSet> call() {
			@Override public CloseableIteration<BindingSet, QueryEvaluationException> call() {
//				return evaluate(joinArg, bindings);
				try {
					return evaluate(joinArg, bindings);
				} catch (QueryEvaluationException e) {
					e.printStackTrace();
					return null;
				}
			}
		};
//		Future<Cursor<BindingSet>> future = executor.submit(callable);
		Future<CloseableIteration<BindingSet, QueryEvaluationException>> future = executor.submit(callable);
		return new AsyncCursor<BindingSet>(future);
	}
	
	class SourceCollector extends QueryModelVisitorBase<RuntimeException> {
		
		Set<Graph> sources = new HashSet<Graph>();
		
		public Set<Graph> getSources(QueryModelNode node) {
			synchronized (this) {
				sources.clear();
				node.visit(this);
				return sources;
			}
		}

		@Override
		public void meet(StatementPattern pattern) throws RuntimeException {
			if (pattern instanceof MappedStatementPattern) {
				sources.addAll(((MappedStatementPattern) pattern).getSources());
			} else {
				throw new IllegalArgumentException("pattern has no source");
			}
		}
		
	}
	
	static class PatternCollector extends QueryModelVisitorBase<RuntimeException> {
		
		Set<StatementPattern> patternSet = new HashSet<StatementPattern>();
		
		public static Set<StatementPattern> getPattern(QueryModelNode node) {
			PatternCollector collector = new PatternCollector();
			node.visit(collector);
			return collector.patternSet;
		}

		@Override
		public void meet(StatementPattern pattern) throws RuntimeException {
			this.patternSet.add(pattern);
		}		
	}

}
