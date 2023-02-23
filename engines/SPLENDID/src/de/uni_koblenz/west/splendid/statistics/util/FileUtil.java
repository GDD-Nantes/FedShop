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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplifies file handling.
 * 
 * @author Olaf Goerlitz
 */
public class FileUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);
	
	public static InputStream openFile(String filename) throws FileNotFoundException {
		
		InputStream input = null;
		
//		try {
			// handle ZIP files
			if (filename.toLowerCase().endsWith(".zip")) {
				
				ZipFile zf;
				try {
					zf = new ZipFile(filename);
				} catch (IOException e) {
					throw new FileNotFoundException(e.getMessage());
				}
				
				Enumeration<? extends ZipEntry> entries = zf.entries();
				while (entries.hasMoreElements()) {
					
					if (input != null) {
						LOGGER.warn("Zip file contains multiple files, extracting only first one: " + filename);
						break;
					}
					
					ZipEntry entry = entries.nextElement();
			    	if (!entry.isDirectory()) {
			    		try {
							input = zf.getInputStream(entry);
						} catch (Exception e) {
							LOGGER.warn("Can not read zip entry: " + entry.getName(), e);
						}
			    	}
			    }
			} else {
				input = new FileInputStream(filename);
			}
//		} catch (FileNotFoundException e) {
//			LOGGER.warn("Unable to file file: " + filename, e);
//		}
		return input;
	}

}
