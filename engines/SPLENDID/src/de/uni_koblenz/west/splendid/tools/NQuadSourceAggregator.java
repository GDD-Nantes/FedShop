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
package de.uni_koblenz.west.splendid.tools;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
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
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * The source information (context URI) in NQuads, with several subdomains and
 * long path fragments, is reduced to a common minimal host name by completely
 * omitting the path information and truncating the first subdomain.
 * 
 * @author goerlitz@uni-koblenz.de
 */
public class NQuadSourceAggregator {
	
	static final String USAGE = "NQuadSourceAggregator [-h] -o <outfile> -i <infile> [<infile2> ...]";
	static final String LINE_SEP = System.getProperty("line.separator");
	
	static final Options OPTIONS    = new Options();
	static final Option HELP        = new Option("h", "help", false, "print this message");
	static final Option OUTPUT_FILE = OptionBuilder
			.hasArg().withArgName("outfile")
			.withDescription("use given file for output (append .gz for Gzipped output); defaults to console output")
			.create("o");
	
	static final Option INPUT_FILES = OptionBuilder
			.hasArg().withArgName("infiles").hasArgs()
			.withDescription("use given files for input (append .gz for Gzipped input); defaults to console input")
			.create("i");
	
	private Counter<Resource> ctxCounter = new Counter<Resource>();
	
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
	        new NQuadSourceAggregator().process(outputFile, inputFiles);
	        
		} catch (ParseException exp) {
			// print parse error and display usage message
			System.out.println(exp.getMessage());
			new HelpFormatter().printUsage(new PrintWriter(System.out, true), 80, USAGE, OPTIONS);
		}
	}
	
	// --------------------------------------------------------------
	
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
			Writer writer = new BufferedWriter(new OutputStreamWriter(getOutputStream(outputFile)), 50000);

			// handle all input streams
			for (String input : inputFiles) {
				
				try {
					InputStream in = getInputStream(input);
					process(in, writer);
					in.close();
				} catch (IOException e) {
					System.err.println("cannot process " + e.getMessage());
				}
			}
			
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("time elapsed: " + ((System.currentTimeMillis() - start) / 1000) + " seconds.");
		System.out.println("reduced to " + ctxCounter.countMap.size() + " contexts.");
	}
	
	private void process(InputStream in, Writer writer) {
		
		NxParser parser = new NxParser(in);
		
		Node[] quad = null;
		while (parser.hasNext()) {
			try {
				quad = parser.next();
				
				// ignore path information, just consider host name
				String host = new URI(quad[3].toString()).getHost();

				if (!isIPAddress(host))
					host = truncateHostName(host);
				
				quad[3] = new Resource("http://"+host);
				
				ctxCounter.add((Resource) quad[3]);
				
				writer.write(quad[0].toN3());
				writer.write(" ");
				writer.write(quad[1].toN3());
				writer.write(" ");
				writer.write(quad[2].toN3());
				writer.write(" ");
				writer.write(quad[3].toN3());
				writer.write(" .");
				writer.write(LINE_SEP);
				
			} catch (URISyntaxException e) {
				System.out.println("Invalid URI: " + e.getMessage());
				continue;
			} catch (Exception e) {
				System.out.println("ERROR: " + e.getClass() + " - " + e.getMessage());
				continue;
			}
		}
	}
	
	/**
	 * Checks if the host name is an IP address.
	 * 
	 * @param host the host name to check.
	 * @return true is the host name is an IP address; false otherwise.
	 */
	private boolean isIPAddress(String host) {
		for (char c : host.toCharArray()) {
			if (!Character.isDigit(c) && !(c=='.')) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Removes the first subdomain from host name.
	 * 
	 * @param host the host name to truncate.
	 * @return the updated host name.
	 */
	private String truncateHostName(String host) {
		// find dot separator after first subdomain name
		int firstDot = host.indexOf(".");
		if (firstDot > 0) {
			// check for '.co.uk' TLD, don't remove domain name 
			if (host.endsWith("co.uk") && (host.length() - firstDot) == 6) {
				return host;
			}
			// check if there is still a domain name left
			if (host.indexOf(".", firstDot+1) > -1 )
				host = host.substring(firstDot+1);
		}
		return host;
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
