import logging
import re
from typing import Union
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser as NTripleParser, DummySink as Sink, ParseError
from rdflib.util import from_n3
from rdflib import BNode, Literal, URIRef, Variable, Graph, Namespace
from rdflib.graph import Graph

import codecs
import sys

import click
import os

Node = Union[URIRef, Literal, BNode]


class NTParser(NTripleParser):

    def skipparse(self, f):
        """Parse f as an N-Triples/N3 file."""
        if not hasattr(f, 'read'):
            raise ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        self.file = codecs.getreader('utf-8')(f)
        self.buffer = ''

        while True:
            self.line = self.readline()
            if self.line is None:
                break
            try:
                self.parseline()
            except ParseError:
                logging.warning(f"parse error: dropping {self.line}. Reason {sys.exc_info()[0]}")
                continue
        return self.sink


class SplitSink(Sink):

    def __init__(self, outdir):
        self.currentfile = None
        os.makedirs(outdir, exist_ok=True)
        self.outdir = outdir

    def triple(self, s: Node, p: Node, o: Node):
        tofile = re.split(r"#|/", s.toPython())[-1]
        #domain = URIRef(s.toPython().rsplit('/', 1)[0])
        if self.currentfile is None:
            currentfile = open(os.path.join(self.outdir, f"{tofile}.nt"), "a")
        if (f"{tofile}.nt" != os.path.basename(currentfile.name)):
            currentfile.close()
            currentfile = open(os.path.join(self.outdir, f"{tofile}.nt"), "a")
        currentfile.write(f"{s.n3()}\t{p.n3()}\t{o.n3()}\t.\n")


@click.command()
@click.argument("infile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("outdir", type=click.Path(dir_okay=True, file_okay=False))
def split(infile, outdir):
    sink = SplitSink(outdir=outdir)
    n = NTParser(sink)
    with open(infile, "rb") as input_file:
        n.skipparse(input_file)


if __name__ == '__main__':
    split()
