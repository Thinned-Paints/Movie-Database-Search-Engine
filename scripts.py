import itertools
import csv
import json
import traceback
from copy import copy
import requests
import elasticsearch
from copy import *
import SearchEngine
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import bulk
elas = Elasticsearch([
    {'host': 'localhost'},
    ])


def get1000():
    """
    I am going for simple variables here, bigolcsv stands for well, a big 'ol csv, and uh, cee ess vee is phonetic
    The reason I am getting the first 1000 using python, is because I am bad at elasticsearch, but somewhat less bad
    at python.
    """
    lines = 1001
    first1000 = []
    try:
        with open("first1000.csv", 'r', encoding='utf8') as bigolcsv:
            ceeessvee = csv.reader(bigolcsv)
            x=0
            for row in itertools.islice(ceeessvee, lines):
                if x>0:
                    first1000.append(row)
                else:
                    pass
                x+=1
    except:
        print("I can't see the first1000.csv file, this is probably your fault. \nPlease put it back and stop meddling\n",
              "Terminating")
        exit(1)
    return first1000

def passbyvalue(object):
    """
    The fact that python makes me use copy bothers me, so I replaced it with something that makes the code easier for me
    to read
    """
    newobject = deepcopy(object)
    return newobject

url = 'http://localhost:9200/wikiplotsindex/'

def populate():
    '''
    This is a mega massive function, as it generates the index, it then does the mapping, extracts the first1000 results
    from the csv, uploads them and runs the gui.
    It's kinda bulky, but it works
    '''
    if elas.indices.exists(index="wikiplotsindex"):
        print("Exists check passed")
        elas.indices.open(index="wikiplotsindex")
        elas.indices.delete(index="wikiplotsindex")
        elas.indices.create(index="wikiplotsindex")
    else:
        print("Exists check failed")
        elas.indices.create(index="wikiplotsindex")

    elas.indices.close(index="wikiplotsindex")
    elas.indices.put_settings(index='wikiplotsindex', body={
        "settings": {
            "index": {
                "similarity": {
                    "default": {
                        "type": "LMJelinekMercer"
                    }
                }
            }
            }}
        )
    elas.indices.open(index="wikiplotsindex")
    analyser = "english"
    elas.indices.put_mapping(index='wikiplotsindex', doc_type='json',include_type_name=True, body={
        "properties": {
                "Cast": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }},
                "Director": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }},
                "Genre": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }},
                "Origin/Ethnicity": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }},
                "Plot": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }
                         },
                "Release Year": {"type": "integer"},
                "Title": {"type": "text", "analyzer": analyser, "fields":{
                    "simple":{
                    "type":"text",
                    "analyzer":"simple"}
                }},
                "Wiki Page": {"type": "keyword"}
            }
    })

    jsonizer(None)
    with open("json.json", "r", encoding="utf-8") as jsonfile:
        jsons = json.load(jsonfile)

        helpers.bulk(elas, jsons, index='wikiplotsindex')

        SearchEngine.gui()

'''
This might be considered global abuse, but I call it creative thinking, these are refrenced in query.
'''
fieldname = ""
searchterm = ""

def query(fn, st):
    '''
    This just runs a query, fieldname and searchterm are the args
    '''
    global fieldname
    global searchterm
    fieldname = fn
    searchterm = st
    if fn=="General":
        result = elas.search(index='wikiplotsindex', body={
            "query": {
                "multi_match": {
                    "query" : searchterm,
                    "fields": ["Title","Title.simple",
                               "Director","Director.simple",
                               "Origin-Ethnicity","Origin-Ethnicity.simple",
                               "Cast","Cast.simple",
                               "Plot","Plot.simple"]
                    }
            }
        }
        )
    else:
        result = elas.search(index='wikiplotsindex', body={
            "query": {
                "multi_match": {
                        "query": searchterm,
                        "fields": [
                            fieldname,(fieldname+".simple")
                        ]
                    }}})


    return (cleanup(result))

def fnorm(fn):
    fn = fn.lower()
    if fn[:-1]=="director":
        nfn = fn[:-1]
        nfn[0]= "D"
        fn = nfn
    return fn

def cleanup(qresult):
    '''
    This takes the byzantine result of a elasticsearch query and renders it into a 2D array, which I find far easier
    to work with.
    '''
    returnar = []

    hits = qresult['hits']['hits']
    if len(hits)==0:
        return "No results"
    for num,doc in enumerate(hits):
        arrow = []
        print("\n",num, '   ', doc)
        releaseyear = doc['_source']['Release Year']
        title = doc['_source']['Title']
        origin = doc['_source']['Origin-Ethnicity']
        Director = doc['_source']['Director']
        Cast = doc['_source']['Cast']
        genre = doc['_source']['Genre']
        wiki = doc['_source']['Wiki Page']
        plot = doc['_source']['Plot']

        arrow.append(releaseyear)
        arrow.append(title)
        arrow.append(origin)
        arrow.append(Director)
        arrow.append(Cast)
        arrow.append(genre)
        arrow.append(wiki)
        arrow.append(plot)

        returnar.append(passbyvalue(arrow))

    cleanresult = returnar
    return printer(cleanresult)

def printer(resultarray):
    '''
    This takes a 2D array and turns it into a massive string that the textbox in my GUI can output easily
    Bigstring is well, a bigstring.
    '''
    bigstring = ""
    rc = "\n"
    count = 1

    for x in resultarray:
        bigstring += "RESULT "+str(count)+rc
        bigstring += "Release Year - "+x[0]+rc
        bigstring += "Title - "+x[1]+rc
        bigstring += "Origin/Ethnicity - "+x[2]+rc
        bigstring += "Director - "+x[3]+rc
        bigstring += "Cast - "+x[4]+rc
        bigstring += "Genre - "+x[5]+rc
        bigstring += "Wiki Link - "+x[6]+rc
        bigstring += "Plot - "+x[7]+rc
        bigstring += rc
        count+=1

    return bigstring

def jsonizer(args):
    '''
    This is what actually generates the JSON via string manipulation
    You may think it's fragile, clunky and excessive, and you'd be right
    But would you just think of how cool the function name is?
    '''
    f1000 = get1000()
    with open("json.json", "w", encoding="utf-8") as file:
        file.write("[\n")
        count = 0
        for x in f1000:
            count+=1

            file.write(" {\n")
            litryear = x[0]
            ryear = '   "Release Year": "'+litryear+'",\n'
            file.write(ryear)

            littitle = x[1]
            title = '   "Title":  "'+littitle+'",\n'
            file.write(title)

            litorigin = x[2]
            origin = '   "Origin-Ethnicity":  "'+litorigin+'",\n'
            file.write(origin)

            litdirector = x[3]
            litdirector = litdirector.replace('"',"")
            director = '   "Director":  "'+litdirector+'",\n'
            file.write(director)

            litcast = x[4]
            litcast = litcast.replace('"',"")
            cast = '   "Cast":  "'+litcast+'",\n'
            file.write(cast)

            litgenre = x[5]
            genre = '   "Genre":  "'+litgenre+'",\n'
            file.write(genre)

            litpage = x[6]
            page = '   "Wiki Page": "'+litpage+'",\n'
            file.write(page)

            litplot = x[7]
            litplot = litplot.replace("\n","")
            litplot = litplot.replace('"',"")
            plot = '   "Plot": "'+litplot+'"\n'
            file.write(plot)

            if (count <= 999):
                file.write("},\n")
            else:
                file.write("}\n")
        file.write("]")
