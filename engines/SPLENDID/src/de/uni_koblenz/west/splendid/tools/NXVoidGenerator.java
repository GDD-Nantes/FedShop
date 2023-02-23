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
package de.uni_koblenz.west.splendid.tools;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import de.uni_koblenz.west.splendid.statistics.util.CompactBNodeTurtleWriter;
import de.uni_koblenz.west.splendid.vocabulary.VOID2;

/**
 * 
 * @author goerlitz@uni-koblenz.de
 */
public class NXVoidGenerator {
	
	static final String USAGE = "NXVoidGenerator [-h] -o <outfile> -i <infile> [<infile2> ...]";
	
	static final Options OPTIONS    = new Options();
	static final Option HELP        = new Option("h", "help", false, "print this message");
	static final Option OUTPUT_FILE = OptionBuilder
			.hasArg().withArgName("outfile")
			.withDescription("use given file for output (append .gz for Gzipped output); defaults to console output")
			.create("o");
	
	static final Option INPUT_FILES = OptionBuilder
			.hasArg().withArgName("infiles").hasArgs()
			.withDescription("use given files for input (append .gz for Gzipped input)")
			.create("i");
	
	static {
		OPTIONS.addOption(HELP);
		OPTIONS.addOption(OUTPUT_FILE);
		OPTIONS.addOption(INPUT_FILES);
	}
	
	public static void main(String[] args) {
		
	    try {
	        // parse the command line arguments
	    	CommandLineParser parser = new GnuParser();
			CommandLine cmd = parser.parse(OPTIONS, args);
	        
	        // print help message
	        if (cmd.hasOption("h") || cmd.hasOption("help")) {
	    		new HelpFormatter().printHelp(USAGE, OPTIONS);
	    		System.exit(0);
	        }
	        
	        // get input files (from option -i or all remaining parameters)
	        String[] inputFiles = cmd.getOptionValues("i");
	        if (inputFiles == null)
	        	inputFiles = cmd.getArgs();
	        if (inputFiles.length == 0) {
	        	System.out.println("need at least one input file.");
				new HelpFormatter().printUsage(new PrintWriter(System.out, true), 80, USAGE);
				System.exit(1);
	        }
	        String outputFile = cmd.getOptionValue("o");
	        
	        // process all input files
	        new NXVoidGenerator().process(outputFile, inputFiles);
	        
		} catch (ParseException exp) {
			// print parse error and display usage message
			System.out.println(exp.getMessage());
			new HelpFormatter().printUsage(new PrintWriter(System.out, true), 80, USAGE, OPTIONS);
		}
	}
	
	// --------------------------------------------------------------

	private static final ValueFactory vf = SimpleValueFactory.getInstance();
	private static final IRI DATASET = vf.createIRI(VOID2.Dataset.toString());
	private static final IRI TRIPLES = vf.createIRI(VOID2.triples.toString());
	private static final IRI CLASSES = vf.createIRI(VOID2.classes.toString());
	private static final IRI ENDTITIES= vf.createIRI(VOID2.entities.toString());
	private static final IRI PROPERTIES = vf.createIRI(VOID2.properties.toString());
	
	Node lastContext = null;
	Set<Node> contexts = new HashSet<Node>();
	long totalTripleCount = 0;
	long tripleCount = 0;
	long contextCount = 0;
	Map<Node, Integer> pMap = new HashMap<Node, Integer>();
	Counter<Integer> predCount = new Counter<Integer>();
	Counter<Node> typeCount = new Counter<Node>();
	
	RDFWriter writer = null;
	
	public void process(String outputFile, String[] inputFiles) {
		
		// sanity check, output file should not be listed as input file
		for (String inputFile : inputFiles) {
			if (inputFile.equals(outputFile)) {
				System.err.println("output file must not overwrite input file");
				return;
			}
		}
		
		long start = System.currentTimeMillis();
		
		try {
			// prepare output file
			this.writer = new CompactBNodeTurtleWriter(getOutputStream(outputFile));
			writer.startRDF();
			writer.handleNamespace("void", "http://rdfs.org/ns/void#");
			writer.handleNamespace("rdf", RDF.NAMESPACE);
			writer.handleNamespace("rdfs", RDFS.NAMESPACE);
			
			// process all input files
			for (String input : inputFiles) {
				process(input);
			}
			
			writer.endRDF();
			
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("cannot write " + e.getMessage());
		}
		
		System.out.println("time elapsed: " + ((System.currentTimeMillis() - start) / 1000) + " seconds.");
	}
	
	private void process(String input) {
		
		System.out.println("processing " + input);
		
		try {
			InputStream in = getInputStream(input);
			NxParser parser = new NxParser(in);

			Node[] quad = null;
			while (parser.hasNext()) {
				quad = parser.next();

				totalTripleCount++;

				Node ctx = quad[3];

				// check context order consistency
				if (isUnorderedContext(ctx)) {
					System.err.println("aborting: " + input + " is not ordered by context (line " + totalTripleCount + ", ctx=" + ctx + ")");
					System.exit(1);
				}
				
				// check if context differs from last context
				if (!ctx.equals(lastContext)) {
					try {
						postProcess(lastContext);
					} catch (RDFHandlerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					addContext(ctx);
				}
				
				handleStatement(quad[0], quad[1], quad[2]);
			}

			in.close();
		} catch (IOException e) {
			System.err.println("cannot read " + e.getMessage());
		}
	}
	
	private void handleStatement(Node s, Node p, Node o) {
		tripleCount++;
		
		// build predicate map
		Integer pID = getPredicateID(p);
		predCount.add(pID);
		
		// test if rdf:type
		if (p.toString().equals(RDF.TYPE.toString())) {
			typeCount.add(o);
		}
		
		// store predicate ID for subject and object
	}
	
	private Integer getPredicateID(Node p) {
		Integer pID = pMap.get(p);
		if (pID == null) {
			pID = pMap.size() + 1;
			pMap.put(p, pID);
		}
		return pID;
	}
	
	private void addPredID() {
		
	}
	
	/**
	 * Checks if the data is not ordered by context.
	 * 
	 * @param context the current context.
	 * @return true if not ordered by context; false otherwise.
	 */
	private boolean isUnorderedContext(Node context) {
		return (!context.equals(lastContext) && contexts.contains(context));
	}
	
	private void postProcess(Node context) throws RDFHandlerException {
		if (context == null)
			return;  // nothing to do
		
		IRI dataset = vf.createIRI(context.toString());
		
		// general void information
		writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, DATASET));
		writer.handleStatement(vf.createStatement(dataset, TRIPLES, vf.createLiteral(String.valueOf(tripleCount))));
		writer.handleStatement(vf.createStatement(dataset, PROPERTIES, vf.createLiteral(String.valueOf(predCount.size()))));
		
		List<Node> keys = new ArrayList<Node>(pMap.keySet());
		Collections.sort(keys);
		for (Node n : keys) {
			try {
				IRI predicate = vf.createIRI(n.toString());
				writePredicateStatToVoid(dataset, predicate, predCount.countMap.get(pMap.get(n)), 0, 0);
			} catch (IllegalArgumentException e) {
				System.err.println("bad predicate: " + e.getMessage());
				continue;
			}
		}
		
		keys = new ArrayList<Node>(typeCount.countMap.keySet());
		Collections.sort(keys);
		for (Node n : keys) {
			try {
				IRI type = vf.createIRI(n.toString());
				writeTypeStatToVoid(dataset, type, typeCount.countMap.get(n));
			} catch (IllegalArgumentException e) {
				System.err.println("bad type: " + e.getMessage());
				continue;
			}
		}
		
//		writer.handleStatement(vf.createStatement(dataset, vf.createIRI(VOID2.classes.toString()), vf.createLiteral(String.valueOf(typeCountMap.size()))));
//		writer.handleStatement(vf.createStatement(dataset, vf.createIRI(VOID2.entities.toString()), vf.createLiteral(String.valueOf(entityCount))));
		
//		System.out.println("Context [" + contextCount + "] " + context + " has " + predCount.size() + " distinct predicates, " + tripleCount + " triples.");
		
		// reset counters etc.
		tripleCount = 0;
		predCount = new Counter<Integer>();
		pMap = new HashMap<Node, Integer>();
	}
	
	private void addContext(Node ctx) {
		contexts.add(ctx);
		lastContext = ctx;
		contextCount++;
	}
	
	public OutputStream getOutputStream(String file) throws IOException {
		if (file == null)
			return System.out;
		
		// TODO: check if file already exists and should be overwritten
		
		OutputStream out = new FileOutputStream(file);
		if (file.endsWith(".gz")) {
			out = new GZIPOutputStream(out);
		}
		return out;
	}
	
	public InputStream getInputStream(String file) throws IOException {
		if (file == null)
			return System.in;
		
		InputStream in = new FileInputStream(file);
		if (file.endsWith("gz")) {
			in = new GZIPInputStream(in);
		}
		return in;
	}
	
	// --------------------------------------------------------------
	
	private void writePredicateStatToVoid(IRI dataset, IRI predicate, long pCount, int distS, int distO) {
		BNode propPartition = vf.createBNode();
		Literal count = vf.createLiteral(String.valueOf(pCount));
		Literal distinctS  = vf.createLiteral(String.valueOf(distS));
		Literal distinctO  = vf.createLiteral(String.valueOf(distO));
		try {
			writer.handleStatement(vf.createStatement(dataset, vf.createIRI(VOID2.propertyPartition.toString()), propPartition));
			writer.handleStatement(vf.createStatement(propPartition, vf.createIRI(VOID2.property.toString()), predicate));
			writer.handleStatement(vf.createStatement(propPartition, vf.createIRI(VOID2.triples.toString()), count));
			writer.handleStatement(vf.createStatement(propPartition, vf.createIRI(VOID2.distinctSubjects.toString()), distinctS));
			writer.handleStatement(vf.createStatement(propPartition, vf.createIRI(VOID2.distinctObjects.toString()), distinctO));
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void writeTypeStatToVoid(IRI dataset, Value type, long tCount) {
		BNode classPartition = vf.createBNode();
		Literal count = vf.createLiteral(String.valueOf(tCount));
		try {
			writer.handleStatement(vf.createStatement(dataset, vf.createIRI(VOID2.classPartition.toString()), classPartition));
			writer.handleStatement(vf.createStatement(classPartition, vf.createIRI(VOID2.clazz.toString()), type));
			writer.handleStatement(vf.createStatement(classPartition, vf.createIRI(VOID2.entities.toString()), count));
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// --------------------------------------------------------------
	
	/**
	 * Simple counting class.
	 * 
	 * @param <T>
	 */
	class Counter<T> {
		
		Map<T, Integer> countMap = new HashMap<T, Integer>();
		
		public void add(T item) {
			Integer count = countMap.get(item);
			if (count == null)
				countMap.put(item, 1);
			else
				countMap.put(item, count+1);
		}
		
		public int size() {
			return countMap.size();
		}
		
	}

}
