from pathlib import Path
from typing import Union
from rdflib.plugins.parsers.ntriples import DummySink as Sink
from rdflib.util import from_n3
from rdflib import Namespace
from rdflib.graph import Graph
from rdflib.term import Literal, URIRef, BNode

import codecs
import sys
import os

import logging
import coloredlogs

import click
import re

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

Node = Union[URIRef, Literal, BNode]

from splitter import NTParser

class AggSink(Sink):
    
    def __init__(self, domain, indir, oufile, files):
        #os.makedirs(outdir, exist_ok=True)
        self.domain: URIRef = domain
        self.indir = indir
        self.outfile = oufile
        self.files = files

    def triple(self, s: Node, p: Node, o: Node):
        #print(f"{s.n3()} {p.n3()} {o.n3()} .")

        with open(self.outfile, "a") as output:
            subject_name = re.split(r"#|/", s.toPython())[-1]
            subject_renamed = URIRef(f"{self.domain.toPython()}/{subject_name}")

            object_renamed = o
            if isinstance(o, URIRef):
                object_name = re.split(r"#|/", o.toPython())[-1]
                object_renamed = URIRef(f"{self.domain.toPython()}/{object_name}")

                # Foutre le sameAs
                sameAs = URIRef('http://www.w3.org/2002/07/owl#sameAs')
                output.write(f"{subject_renamed.n3()}\t{sameAs.n3()}\t{s.n3()}\t{self.domain.n3()}.\n")
                output.write(f"{object_renamed.n3()}\t{sameAs.n3()}\t{o.n3()}\t{self.domain.n3()}.\n")

            output.write(f"{subject_renamed.n3()}\t{p.n3()}\t{object_renamed.n3()}\t{self.domain.n3()}.\n")

        if isinstance(o, URIRef):
        #if str(o).startswith("http://"):
            objectfile = os.path.join(self.indir, f"{re.split(r'#|/', o.toPython())[-1]}.nt")
            if (os.path.exists(objectfile) and (objectfile not in self.files)):
                self.files.append(objectfile)
                logger.info(f"Reading file: {objectfile}")
                with open(objectfile,"rb") as input:
                    NTParser(AggSink(domain=self.domain, indir=self.indir, oufile=self.outfile, files=self.files)).skipparse(input)
        return self.files

@click.command()
@click.argument("infile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("indir", type=click.Path(dir_okay=True, file_okay=False))
@click.argument("outfile", type=click.Path(dir_okay=False, file_okay=True))
@click.argument("domain", type=click.STRING)
def aggregate(infile, indir, outfile, domain):
    sink=AggSink(domain=URIRef(domain), indir=indir,oufile=outfile, files=[])
    n=NTParser(sink)
    with open(infile, "rb") as input_file:
        n.skipparse(input_file)

if __name__ == '__main__':
    aggregate()