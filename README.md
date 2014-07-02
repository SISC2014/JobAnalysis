JobAnalysis
===========

Visualizes data taken from a Mongo DB of condor jobs from OSG Connect



condor_retrieval.py - retrieves currently running job data from collectors and stores them into a mongo database
config.ini - required for condor_retrieval (contains list of collectors and wait intervals)

MongoRetrieval/src/EfficiencyHistogram.py - creates histograms of efficiencies of condor jobs
MongoRetrieval/src/JSONEncoder.py - practice program to encode data from mongo into JSON document
MongoRetrieval/src/ListOfSites.txt - required for JSONEncoder.py
MongoRetrieval/src/__pycache__ - required for JSONEncoder.py

efficiency-hisogram.wsgi - practice program to create wsgi for mongo data
monog-retrieval.wsgi - takes data from mongo and outputs it as JSON docs
