import pandas as pd
from pybliometrics.scopus import AbstractRetrieval, AuthorRetrieval
import networkx as nx
import pickle
import matplotlib.pyplot as plt
import traceback
from datetime import datetime
from collections import Counter


def quickpickle(obj, filepath):
    '''straightforward wrapper for pickle.dump'''
    pickle.dump(obj, open(filepath, 'wb'))

def quickunpickle(filepath):
    '''wrapper for pickle.load'''
    with open(filepath,'rb') as f:
        obj = pickle.load(f)
    return obj
    
class ThesisSession:
    '''this class is a container for code used by
    Michael Rebstock while compiling data for his 
    Master's Thesis.  It pulls data from the Scopus
    database and generates NetworkX graphs and some 
    pandas dataframes.'''
    
    def __init__(self, documents = []):
        self.documents = documents
        self.fails = []
        self.authorship_graph = nx.DiGraph()
        self.citation_graph = nx.DiGraph()
        self.target_depth = 0
        self.last_quota = ''
        self.reset_time = ''
        
        
    def __str__(self):
        return (
            f'{len(self.documents)} documents pulled.\n'
            f'{len(self.fails)} failed pulls.\n'
            f'{len(self.citation_graph)} total publications graphed.\n'
            f'{len(self.authorship_graph)} total nodes graphed.\n'
            f'Current target depth = {self.target_depth}.'
        )
                  
    def show_progress(self, place, end):
        '''this tells the user how much of a process is complete.'''
        progress = place/end*100
        print(f"Progress: {progress: .2f}%", end="\r")
      
    def eid_from_id(self, thisid):
        '''scopus uses an id and an eid.  the eid is the id with a prefix added.
        this function simply appends said prefix to a provided id'''
        eid_prefix = '2-s2.0-' 
        return eid_prefix+thisid
    
    def add_citation(self, cited_by, reference):
        '''both args these are Elsevier EIDs'''
        self.citation_graph.add_edge(cited_by, reference)
        
    def is_repeat(self,eid):
        '''checks if an eid has already been pulled in order to 
        prevent them from being pulled again.'''
        answer = False
        match = None
        
        for doc in self.documents:
            if doc.eid == eid:
                answer=True
                match = doc
         
        return answer, match
        
    def has_documents(self):
        '''returns True if any documents have been successfully pulled.'''
        answer = True
        
        if len(self.documents)==0:
            answer = False
            print("Do documents added.")
            
        return answer

    def pull_abstract(self, eid, depth, report = False, flags={}):
        '''this function does the heavy lifting, using the pybliometircs
        AbstractRetrieval Class to pull data about a document identified 
        by its eid.  If depth is less than the target depth (default 0),
        then it will also pull data about that document's references, if
        available.'''
        min_indegree_to_pull = 3
        
        def pull_references(abstract):
            if abstract.references is not None:
                #add edges to the citation network.
                for reference in abstract.references:
                    reference_eid = self.eid_from_id(reference.id)
                    
                    self.citation_graph.add_edge(abstract.eid, reference_eid)
                    #we don't yet know anything about the reference beyond its id.  
                    #If it gets pulled later, attributes will be associated with this node instead of creating a new one.
                    if (depth < self.target_depth):
                        #depending on target_depth, maybe pull references
                        if self.citation_graph.in_degree(reference_eid) >= min_indegree_to_pull:
                            # only pull if you've seen this document enough times.
                            self.pull_abstract(reference_eid, depth+1)        
        
        found, match = self.is_repeat(eid)
        if found==False:
            try: 
                abstract = AbstractRetrieval(eid, view='FULL')
                if abstract is not None:
                    #update quota information
                    self.last_quota = abstract.get_key_remaining_quota()
                    self.reset_time = abstract.get_key_reset_time()
                    
                    #add this object to the list of document objects
                    self.documents.append(abstract)
                    flags.update(year = abstract.coverDate[:4])
                    self.citation_graph.add_node(abstract.eid, flags)
                    if report == True:
                        #print out document data, if directed to do so
                        print(abstract)
                    if abstract.authors is not None:
                        self.authorship_graph.add_node(publication.eid, bipartite = 0)
                        #add edges to the authorship network
                        for author in abstract.authors:
                            self.authorship_graph.add_node(author.auid, bipartite = 1, name = author.indexed_name)
                            self.authorship_graph.add_edge(publication.eid, author.auid)
                    pull_references(abstract)

                                
            except Exception:
                #if anything goes wrong, add the eid to a list of problematic eids
                self.fails.append(eid)
                print(eid,"failed to pull")
                traceback.print_exc()
        else:
            #this eid has already been successfully pulled, so we only want to check depth
            pull_references(match)
    
    def pull_author(self, auid, flags={}):
        '''given an auid, pulls all of the publications of that author'''
        
        try:
            author = AuthorRetrieval(auid, view='LIGHT')
            if author is not None:
                newdocs = author.get_document_eids()
                if newdocs is not None:
                    self.add_documents(newdocs, flags)
        except Exception:
            print(auid,"failed to pull")
            traceback.print_exc()    

    def pull_authors(self, auidlist, flags={}):
        '''pull_author(), but for a list of auids'''
        if isinstance(auidlist,str):
            auidlist = [auidlist]
        
        for index, auid in enumerate(auidlist):
            self.pull_author(auid, flags)
            self.show_progress(index+1,len(auidlist))       
    
    def compile_graphs(self, filepath, target_depth = False, flags={}):
        '''takes a file with a list of eids and a specified depth, and 
        populates the object's properties.'''
        
        with open(filepath, 'r') as f:
            outlist = f.read().splitlines()
        
        self.add_documents(outlist,target_depth, flags)
        
    def add_document(self, eid, report = True, flags={}):
        '''given an eid, pulls data about that document and adds it
        to the object's properties.  Basically a single-item version
        of compile_graphs()'''
        self.pull_abstract(eid, 0, report, flags) #depth starts at zero
        
    def add_documents(self, eidlist, target_depth = False, flags={}):
        '''add_document, but takes a list.'''
        if isinstance(eidlist, str):
            eidlist = [eidlist]
            
        self.target_depth = target_depth if target_depth else self.target_depth            
            
        for index, eid in enumerate(eidlist):
            self.add_document(eid, report = False, flags)
            self.show_progress(index+1,len(eidlist))           
       
    def save_checkpoint(self, key=None):
        '''pickles the accumulated data from a session.'''
        
        currentdatetime = datetime.now().strftime("%y%m%d-$H%M%S")
        if key is None:
            key = str(currentdatetime)
        
        save_content = dict([
            ('documents' = self.documents), 
            ('authorship' = self.authorship_graph), 
            ('citations' = self.citation_graph)
            ])
        filepath = './archive/session'+key+'.pickle'
        pickle.dump(save_content, open(filepath, 'wb'))
        
    def load_checkpoint(self, key=None):
        if key is None:
            print('key or needed to load checkpoint!')
        else:
            filepath = './archive/session'+key+'.pickle'
            with open(filepath,'rb') as f:
                content = pickle.load(f)
            
            self.documents = content['documents']
            self.authorship_graph = content['authorship']
            self.citation_graph = content['citations']
        
    def subject_list(self):
        '''returns a pandas dataframe with information about subject 
        areas represented in the list of documents'''
        subjects = []
        
        if self.has_documents():
            for doc in self.documents:
                if doc.subject_areas is not None:
                    for area in doc.subject_areas:
                        subjects.append(area)
            
            subjects = pd.DataFrame(subjects)
            subjects = areas.value_counts().groupby(['area','abbreviation','code']).sum()
            subjects.sort_values(ascending=False, inplace=True)
            
        return pd.DataFrame(subjects)
        
    def author_list(self):
        '''returns a pandas dataframe with author information'''
        authors = []

        if self.has_documents():        
            for doc in self.documents:
                if doc.authors is not None:
#                    for author in doc.authors:
#                        authors.append(pd.DataFrame(author))
                    authors.append(pd.DataFrame(doc.authors))
                else:
                    print('No authors found for ',doc.eid)
  
            authors = pd.concat(authors, ignore_index=True)
            authors = authors.value_counts().groupby(['auid','indexed_name']).sum()       
            
        return pd.DataFrame(authors)
        
    def doc_years(self):
        '''returns the distribution of publication years for the 
        documents pulled.'''
        years = []
        
        if self.has_documents():
            for doc in self.documents:
                years.append(doc.coverDate[:4])
                
        years = pd.DataFrame({'year':years})

        return years.value_counts().sort_index().reset_index(name='count')
        
    def quotas(self):
        '''relays the latest quota information'''
        reset_string="Reset Time Unknown"
        quota_string="Pulls Remaining Unknown"
        
        if self.reset_time is not None:
            start = datetime.now()
            end = datetime.fromisoformat(str(self.reset_time))
            diff = end - start
            reset_string = f'key resets on {self.reset_time} ({diff})'
            
        if self.last_quota is not None:
            quota_string = f'remaining pulls this key: {self.last_quota}.'
            
        print(quota_string,'\n',reset_string)
        
    def doc_dataframe(self):
        '''NOT IMPLEMENTED:
        returns a dataframe with certain information about the   
        documents that have been pulled.'''
        
        keep_cols = ('eid','title', 'publicationName', 'date', 'cited_by_count', 'doi', 'authkeywords', 'subject_areas', 'abstract', 'scopus_link']
        
        #go through self.documents and pull out important fields
        for doc in self.documents:
                out.append([
                    doc.eid,
                    doc.title,
                    doc.publicationName,
                    doc.coverDate[:4],
                    doc.citedby_count,
                    doc.doi,
                    doc.authkeywords,
                    doc.subject_areas
                    doc.abstract,
                    doc.scopus_link
                   ])        
        
        #build a dataframe from these fields
        df = pd.DataFrame(out, columns = keep_cols)
        
        #return the dataframe to the user
        return df
        
    def get_nodes_with_attribute(self, graph, attribute, value):
        '''given a graph, and attribute key and a value, returns a list of nodes
        that have that attribute/value pair.'''
        
        selected = []
        for n, d in graph.nodes().items():
            if attribute in d and d[attribute] == value:
                selected.append(n)
        return selected

    def distance_from_initial_sample(self, node_id):
        '''given a node id, returns an integer with the length of the geodesic 
        between that node id and the closest node that is part of the initial sample.'''
        initial_node_ids = self.get_nodes_with_attribute(self.citation_graph, 'initial', True)
        undirected_view = self.citation_graph.to_undirected(as_view=True)
        distance = nx.diameter(undirected_view)
        for initial_node_id in initial_node_ids:
            this_distance = nx.shortest_path_length(undirected_view, source=initial_node_id, target=node_id)
            distance = min(distance, this_distance)
            
        return distance
            
    def suggest_adds(self, threshold=False):
        '''NOT IMPLEMENTED:
        checks for nodes with high indegree that haven't been pulled.'''
        
        if !threshold:
            #calculate a reasonable threshold
            threshold = 10
        
        #find nodes in self.citation_graph that have an indegree > threshold
        
        #provide the list of eids to the user        

        
