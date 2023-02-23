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
package de.uni_koblenz.west.splendid.statistics.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;

/**
 * Analyzes the structuredness of a dataset as described by Duan et al. in 
 * "Apples and Oranges: A Comparison of RDF Benchmarks and Real RDF Datasets".
 * 
 * IMPORTANT: the input dataset has to be in N-Triples format and sorted by subjects.
 *            unsorted datasets need to be sorted first by 'sort -k1 dataset.nt'. 
 * 
 * @author Olaf Goerlitz
 */
public class StructurednessAnalyzer extends RDFHandlerBase {
	
	private final Map<URI, Map<URI, Integer>> predCountPerType = new HashMap<URI, Map<URI,Integer>>();
	private final Map<URI, Integer> totalTypeCounts = new HashMap<URI, Integer>();
	private final Map<URI, Integer> totalPredCounts = new HashMap<URI, Integer>();
	
	private final Set<URI> typesOfCurrentSubject = new HashSet<URI>();
	private final Set<URI> predsOfCurrentSubject = new HashSet<URI>();
	private Resource lastSubject = null;
	
	private void count(Map<URI, Map<URI, Integer>> countMap, URI key, URI subkey) {
		Map<URI, Integer> subcountMap = countMap.get(key);
		if (subcountMap == null) {
			subcountMap = new HashMap<URI, Integer>();
			countMap.put(key, subcountMap);
		}
		count(subcountMap, subkey);
	}
	
	private void count(Map<URI, Integer> countMap, URI key) {
		Integer count = countMap.get(key);
		if (count == null) {
			countMap.put(key, 1);
		} else {
			countMap.put(key, 1 + count);
		}
	}
	
	/**
	 * Stores types and predicates occoring with the current subject.
	 * 
	 * @param st the Statement to process.
	 */
	private void storeStatement(Statement st) {
		
		URI predicate = st.getPredicate();
		Value object = st.getObject();
		
		// check for type statement
		if (RDF.TYPE.equals(predicate)) {
			typesOfCurrentSubject.add((URI) object);
			count(totalTypeCounts, (URI) object);
		} else {
			predsOfCurrentSubject.add(predicate);
			count(totalPredCounts, predicate);
		}
		
		lastSubject = st.getSubject();
	}
	
	/**
	 * Analyzes the last statements (which have the same subject)
	 * and counts the predicates per type.
	 */
	private void processStoredStatements() {
		if (lastSubject == null)
			return;
		
		// count predicates for all types
		for (URI type : typesOfCurrentSubject) {
			for (URI pred : predsOfCurrentSubject) {
				count(predCountPerType, type, pred);
			}
		}
		
		// clear stored values;
		typesOfCurrentSubject.clear();
		predsOfCurrentSubject.clear();
	}
	
	public double computeCoverage() {
		
		Map<URI, Double> typeCoverage = new HashMap<URI, Double>();
		Map<URI, Integer> wtCovFactor = new HashMap<URI, Integer>();
		
		long coverageSum = 0;
		
		// compute coverage
		for (URI type : predCountPerType.keySet()) {

			long predCountSum = 0;
			Map<URI, Integer> predCounts = predCountPerType.get(type);
			
			// compute sum of all predicate counts
			for (URI pred : predCounts.keySet()) {
				predCountSum += predCounts.get(pred);
			}
			
			int pCount = predCounts.size();
			int iCount = totalTypeCounts.get(type);
			
			typeCoverage.put(type, (double) predCountSum / (pCount * iCount));
			wtCovFactor.put(type, pCount + iCount);
			
			coverageSum += pCount + iCount;
		}
		
		// compute weighted coverage and coherence
		double coherence = 0;
		for (URI type : predCountPerType.keySet()) {
			double weightedCov = (double) wtCovFactor.get(type) / coverageSum;
			coherence += weightedCov * typeCoverage.get(type);
		}
		
		return coherence;
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public void handleStatement(Statement st) throws RDFHandlerException {
		
		// check if current triple has different subject than the last triple
		if (!st.getSubject().equals(lastSubject)) {
			processStoredStatements();
		}
		
		storeStatement(st);
	}
	
	@Override
	public void endRDF() throws RDFHandlerException {
		super.endRDF();

		processStoredStatements();
	}
	
	// ------------------------------------------------------------------------
	
	public static void main(String[] args) throws Exception{

		// check for file parameter
		if (args.length < 1) {
			String className = StructurednessAnalyzer.class.getName();
			System.err.println("USAGE: java " + className + " RDF.nt{.zip}");
			System.exit(1);
		}
		
		// process all files given as parameters
		for (String arg : args) {

			// check if file exists
			File file = new File(arg);
			if (!file.exists()) {
				System.err.println("file not found: " + file);
				System.exit(1);
			}

			// check if file is not a directory
			if (!file.isFile()) {
				System.err.println("not a normal file: " + file);
				System.exit(1);
			}
			
			processFile(file);
		}
	}
	
	public static void processFile(File file) throws IOException {
		
		// check for gzip file
		if (file.getName().toLowerCase().contains(".gz")) {
			processInputStream(new GZIPInputStream(new FileInputStream(file)), file.getName());
		}
		
		// check for zip file
		else if (file.getName().toLowerCase().contains(".zip")) {
			ZipFile zf = new ZipFile(file);
			if (zf.size() > 1) {
				System.out.println("found multiple files in archive, processing only first one.");
			}
			ZipEntry entry = zf.entries().nextElement();
			if (entry.isDirectory()) {
				System.err.println("found directory instead of normal file in archive: " + entry.getName());
				System.exit(1);
			}
			
			processInputStream(zf.getInputStream(entry), entry.getName());
		} 
		
		// process data stream of file
		else {
			processInputStream(new FileInputStream(file), file.getName());
		}
	}
	
	public static void processInputStream(InputStream input, String filename) throws IOException {
		
		long start = System.currentTimeMillis();
		System.out.println("processing " + filename);
		
		// identify parser format
		RDFFormat format = Rio.getParserFormatForFileName(filename);
		if (format == null) {
			System.err.println("can not identify RDF format for: " + filename);
			System.exit(1);
		}
		
		// initalize parser
		StructurednessAnalyzer handler = new StructurednessAnalyzer();
		RDFParser parser = Rio.createParser(format);
		parser.setRDFHandler(handler);
		parser.setStopAtFirstError(false);
		
		try {
			parser.parse(input, "");
		} catch (RDFParseException e) {
			System.err.println("encountered error while parsing " + filename + ": " + e.getMessage());
			System.exit(1);
		} catch (RDFHandlerException e) {
			System.err.println("encountered error while processing " + filename + ": " + e.getMessage());
			System.exit(1);
		}
		finally {
			input.close();
		}
		
		System.out.println((System.currentTimeMillis() - start)/1000 + " seconds elapsed");
		
		System.out.println("Coherence: " + handler.computeCoverage());
	}

}
