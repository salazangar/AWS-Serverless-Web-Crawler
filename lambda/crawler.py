#System
from datetime import datetime
from typing import List
import uuid
import json

#Lambda
import logging

#Project
import boto3

from utilities.util import *

from requests_html import HTMLSession

from models.VisitedURL import VisitedURL

ddb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='Crawler')
table = ddb.Table('VisitedURLs')


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handle(event, context):
    print(event)
    eventDict = json.loads(event["Records"][0]["body"])
    print("Event ^^^ ")
    visitedURL = eventDict["visitedURL"]
    runId = eventDict["runId"]
    sourceURL = visitedURL  #sourceURL becomes visitedURL // early logging
    rootURL = eventDict["rootURL"]

    print(f"Initiating crawl for URL={visitedURL}, runId={runId}, sourceURL={sourceURL}, rootURL={rootURL}")

    # Retrieve all links from URL
    referrals = fetchLinksFromURL(visitedURL) 
    print(f"Fetched {len(referrals)} links from {rootURL}")

    # Filter out links from different domain ( not rooturl)  
    filteredDomainReferrals = filterLinkCandidatesForRootURL(rootURL, referrals)
    print(f"Found {len(filteredDomainReferrals)} referrals sourced from {rootURL}")

    # Retrieve filtered records from above
    visitedLinkRecords = fetchVisitedCandidates(filteredDomainReferrals, runId)
    print(f"Already visited {len(visitedLinkRecords)}")
    visitedLinks = map(lambda record: record["visitedURL"], visitedLinkRecords)

    # Filter out records that are already visited
    remainingCrawlTargets = findUnvisitedLinks(filteredDomainReferrals, visitedLinks)
    print(f"{len(remainingCrawlTargets)} need to be processed")
    
    if (len(remainingCrawlTargets) > 0):
        # Mark all links as visited (eager marking)
        markAllVisited(remainingCrawlTargets, runId, sourceURL, rootURL) # mark them all as visited
        print(f"Marked {len(remainingCrawlTargets)} as visited")

        # Enqueue them all for later processing
        enqueueAll(remainingCrawlTargets, runId, sourceURL, rootURL) # enqueue remaining
        print(f"Completed enqueue of {len(remainingCrawlTargets)} URLs")


def enqueueAll(targets, runId, sourceUrl, rootUrl):
    batchEnqueue(queue, targets, runId, sourceUrl, rootUrl)

def markAllVisited(targets, runId, sourceUrl, rootUrl):
    batchPutItems(table, targets, runId, sourceUrl, rootUrl)
    
def findUnvisitedLinks(potentialLinks, visitedLinks):
    unvisitedLinks = set(potentialLinks).difference(visitedLinks)
    return unvisitedLinks

def filterLinkCandidatesForRootURL(rootUrl: str, linkCandidates: list[str]):
    return list(filter(lambda link: link.startswith(rootUrl) and not ("#" in link), linkCandidates))


def fetchLinksFromURL(link: str) -> list[str]:
    session = HTMLSession()
    r = session.get(link)
    print("Retrieved " + str(len(r.html.links)) + " links")
    return r.html.links

def fetchVisitedCandidates(candidates: list[str], runId: str) -> list[str]:
    return batchGetItems(ddb, candidates, runId)
    

# handle({"Records": {
#    "visitedURL":"https://www.beabetterdev.com",
#    "runId":"2022-04-23 15:48:19.737279#g12eacec-5e84-41dd-991c-1fb041a4069b",
#    "sourceURL":"",
#    "rootURL":"https://www.beabetterdev.com"
# }, None)