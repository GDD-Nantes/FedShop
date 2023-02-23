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
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.eclipse.rdf4j.query.QueryEvaluationException;

//import org.eclipse.rdf4j.cursor.Cursor;
//import org.eclipse.rdf4j.store.StoreException;

/**
 * Allows for asynchronous fetching of the cursor's input data.
 *  
 * @author Olaf Goerlitz
 *
 * @param <E> The type of object that the cursor iterates over.
 */
//public class AsyncCursor<E> implements Cursor<E> {
public class AsyncCursor<E> extends LookAheadIteration<E, QueryEvaluationException> {
	
//	protected Future<Cursor<E>> future;
//	protected Cursor<E> result;
	protected Future<CloseableIteration<E, QueryEvaluationException>> future;
	protected CloseableIteration<E, QueryEvaluationException> result;
	
//	public AsyncCursor(Future<Cursor<E>> future) {
	public AsyncCursor(Future<CloseableIteration<E, QueryEvaluationException>> future) {
		if (future == null)
			throw new IllegalArgumentException("future must not be null");
		
		this.future = future;
	}

////	public AsyncCursor(ExecutorService executor, Callable<Cursor<E>> callable) {
//	public AsyncCursor(ExecutorService executor, Callable<CloseableIteration<E, QueryEvaluationException>> callable) {
//		if (executor == null)
//			throw new IllegalArgumentException("executor must not be null");
//		
//		this.future = executor.submit(callable);
//	}
	
	/**
	 * Stop the evaluation thread and close any open cursor.
	 */
//	public void close() throws StoreException {
	protected void handleClose() throws QueryEvaluationException {
		if (result != null)
			result.close();
		else
			future.cancel(true);
	}
	
//	public BindingSet next() throws StoreException {
	protected E getNextElement() throws QueryEvaluationException {
		try {
			if (result == null)
				result = future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
		
//		return result.next();
		if (result != null && result.hasNext())
			return result.next();
		else
			return null;
	}

}
