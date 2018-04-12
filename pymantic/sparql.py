"""Provide an interface to SPARQL query endpoints."""

import datetime
import urllib
import urlparse

import requests
from lxml import objectify
import pytz
import rdflib
import json

import logging

log = logging.getLogger(__name__)

class SPARQLQueryException(Exception):
    """Raised when the SPARQL store returns an HTTP status code other than 200 OK."""
    pass

class UnknownSPARQLReturnTypeException(Exception):
    """Raised when the SPARQL store provides a response with an unrecognized content-type."""
    pass

class SPARQLServer(object):
    """A server that can run SPARQL queries."""

    def __init__(self, query_url, post_queries = True):
        self.query_url = query_url
        self.post_queries = post_queries
        self.s = requests.Session()

    acceptable_sparql_responses = [
        'application/sparql-results+json',
        'application/rdf+xml',
        'application/sparql-results+xml',
    ]

    def query(self, sparql, output='json'):
        """Executes a SPARQL query. The return type varies based on what the
        SPARQL store responds with:

        * application/rdf+xml: an rdflib.ConjunctiveGraph
        * application/sparql-results+json: A dictionary from simplejson
        * application/sparql-results+xml: An lxml.objectify structure

        :param sparql: The SPARQL to execute.
        :returns: The results of the query from the SPARQL store."""
        log.debug("Querying: %s with: %r", self.query_url, sparql)
        if self.post_queries:
            response = self.s.post(
                self.query_url,
                data={'query': sparql, 'output':output},
                headers={
                    "Accept": ','.join(self.acceptable_sparql_responses),
                },
                stream=True,
            )
        else:
            response = self.s.get(
                self.query_url,
                params={'query': sparql, 'output':output},
                headers={
                    "Accept": ','.join(self.acceptable_sparql_responses),
                },
                stream=True,
            )
        if response.status_code == 204:
            return True
        if response.status_code != 200:
            raise SPARQLQueryException('%s: %s' % (response, response.text))
        if response.headers['content-type'].startswith('application/rdf+xml'):
            graph = rdflib.ConjunctiveGraph()
            graph.parse(response.raw, self.query_url)
            return graph
        elif response.headers['content-type'].startswith(
            'application/sparql-results+json'
        ):
            return json.loads(response.text)
        elif response.headers['content-type'].startswith(
            'application/sparql-results+xml'
        ):
            return objectify.parse(response.raw)
        else:
            raise UnknownSPARQLReturnTypeException(
                'Got content of type: %s' % response.headers['content-type'])


class UpdateableGraphStore(SPARQLServer):
    """SPARQL server class that is capable of interacting with SPARQL 1.1
    graph stores."""

    def __init__(self, query_url, dataset_url, param_style=True, **kwargs):
        super(UpdateableGraphStore, self).__init__(query_url, **kwargs)
        self.dataset_url = dataset_url
        self.param_style = param_style

    acceptable_graph_responses = [
        'text/plain',
        'application/rdf+xml',
        'text/turtle',
        'text/rdf+n3',
    ]

    def request_url(self, graph_uri):
        if self.param_style:
            return self.dataset_url + '?' + urllib.urlencode({'graph': graph_uri})
        else:
            return urlparse.urljoin(self.dataset_url, urllib.quote_plus(graph_uri))

    def get(self, graph_uri):
        resp = self.s.get(
            self.request_url(graph_uri),
            headers={'Accept': ','.join(self.acceptable_graph_responses),},
            stream=True,
        )
        if resp.status_code != 200:
            raise Exception(
                'Error from Graph Store (%s): %s' % (resp.status_code, resp.text),
            )
        graph = rdflib.ConjunctiveGraph()
        if resp.headers['content-type'].startswith('text/plain'):
            graph.parse(resp.raw, publicID=graph_uri, format='nt')
        elif resp.headers['content-type'].startswith('application/rdf+xml'):
            graph.parse(resp.raw, publicID=graph_uri, format='xml')
        elif resp.headers['content-type'].startswith('text/turtle'):
            graph.parse(resp.raw, publicID=graph_uri, format='turtle')
        elif resp.headers['content-type'].startswith('text/rdf+n3'):
            graph.parse(resp.raw, publicID=graph_uri, format='n3')
        return graph

    def delete(self, graph_uri):
        resp = self.s.delete(self.request_url(graph_uri))
        if resp.status_code not in (200, 202):
            raise Exception(
                'Error from Graph Store (%s): %s' % (resp.status_code, resp.text),
            )

    def put(self, graph_uri, graph):
        graph_triples = graph.serialize(format = 'nt')
        resp = self.s.put(
            self.request_url(graph_uri),
            data=graph_triples,
            headers={'content-type': 'text/plain'},
        )
        if resp.status_code not in (200, 201, 204):
            raise Exception(
                'Error from Graph Store (%s): %s' % (resp.status_code, resp.text))

    def post(self, graph_uri, graph):
        graph_triples = graph.serialize(format = 'nt')
        if graph_uri != None:
            resp = self.s.post(
                self.request_url(graph_uri),
                data=graph_triples,
                headers={'content-type': 'text/plain'},
            )
            if resp.status_code not in (200, 201, 204):
                raise Exception('Error from Graph Store (%s): %s' %
                                (resp.status_code, resp.text))
        else:
            resp = self.s.post(
                self.dataset_url,
                data=graph_triples,
                headers={'content-type': 'text/plain'},
            )
            if resp.status_code != 201:
                raise Exception('Error from Graph Store (%s): %s' %
                                (resp.status_code, resp.text))

class PatchableGraphStore(UpdateableGraphStore):
    """A graph store that supports the optional PATCH method of updating
    RDF graphs."""

    def patch(self, graph_uri, changeset):
        graph_xml = changeset.serialize(format = 'xml', encoding='utf-8')
        resp = self.s.patch(
            self.request_url(graph_uri),
            data=graph_xml,
            headers={'content-type': 'application/vnd.talis.changeset+xml'},
        )
        if resp.status_code not in (200, 201, 204):
            raise Exception('Error from Graph Store (%s): %s' %
                            (resp.status_code, resp.text))
        return True

def changeset(a,b, graph_uri):
    """Create an RDF graph with the changeset between graphs a and b"""
    cs = rdflib.Namespace("http://purl.org/vocab/changeset/schema#")
    graph = rdflib.Graph()
    graph.namespace_manager.bind("cs", cs)
    removal, addition = differences(a,b)
    change_set = rdflib.BNode()
    graph.add((change_set, rdflib.RDF.type, cs["ChangeSet"]))
    graph.add((change_set, cs["createdDate"], rdflib.Literal(datetime.datetime.now(pytz.UTC).isoformat())))
    graph.add((change_set, cs["subjectOfChange"], rdflib.URIRef(graph_uri)))
    
    for stmt in removal:
        statement = reify(graph, stmt)
        graph.add((change_set, cs["removal"], statement))
    for stmt in addition:
        statement = reify(graph, stmt)
        graph.add((change_set, cs["addition"], statement))
    return graph

def reify(graph, statement):
    """Add reifed statement to graph"""
    s,p,o = statement
    statement_node = rdflib.BNode()
    graph.add((statement_node, rdflib.RDF.type, rdflib.RDF.Statement))
    graph.add((statement_node, rdflib.RDF.subject,s))
    graph.add((statement_node, rdflib.RDF.predicate, p))
    graph.add((statement_node, rdflib.RDF.object, o))
    return statement_node

def differences(a, b, exclude=[]):
    """Return (removes,adds) excluding statements with a predicate in exclude"""
    exclude = [rdflib.URIRef(excluded) for excluded in exclude]
    return ([s for s in a if s not in b and s[1] not in exclude],
            [s for s in b if s not in a and s[1] not in exclude])
