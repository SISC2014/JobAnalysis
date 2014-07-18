JobAnalysis
===========

__Visualizes data taken from a Mongo DB of condor jobs from OSG Connect__



condor_retrieval.py - retrieves currently running job data from collectors and stores them into a mongo database

config.ini - required for condor_retrieval (contains list of collectors and wait intervals)

MongoRetrieval/src/EfficiencyHistogram.py - creates histograms of efficiencies of condor jobs

MongoRetrieval/src/JSONEncoder.py - practice program to encode data from mongo into JSON document

MongoRetrieval/src/ListOfSites.txt - required for JSONEncoder.py

MongoRetrieval/src/pycache - required for JSONEncoder.py

efficiency-hisogram.wsgi - practice program to create wsgi for mongo data

mongo-retrieval.wsgi -  practice program that takes data from mongo and outputs it as JSON docs

job-retrieval.wsgi - outputs json doc of jobs completed in the last X hours along with their project, user, cluster, start/end time, wall time, cpu time, site, and site coordinates

reality-check.wsgi - returns total wall time for last eight days for user/project/site. goal is to compare with data provided by oim

site-map.wsgi - returns json doc of total wall time, cpu time, and # of jobs, grouped by project, grouped by site
