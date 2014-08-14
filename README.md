#Ceasium

Caesium is a restful resrouce framework with a focus on document scheduling.  Proudly built on top of MongoDB, Tornado, and Motor frameworks.  

##[API Documentation](http://urbn.github.io/Caesium)
##[CaesiumQuickstart](http://github.com/urbn/CaesiumQuickstart)

##Compatibility

Currently it works on python 2.7 only, but 3.3 will be supported soon.

##Installation

Installation is handled the normal way and brings all required dependencies as well.

```
pip install caesium
```

##Build Status

![Travis Build](https://travis-ci.org/urbn/Caesium.svg?branch=master)

#Key Feaures

- Schedule create, update and delete actions at a UTC timestamp, supports document inheritance by default
- Revisions are kept in separate collections per resource type
- Simple to data structure, everything is lists of dictionaries adapted to primitives
- Uses coroutines by default and won't block unless you call yield in your handlers
- Supports JSON Schema models for validation
- Revisions are kept in separate collections <type>_revisions, which can be ideal for some microservice implementations

##Document API (ASyncMotorDocument)

One of the simplest to use portions of the frame work is the ASyncMotorDocument class. 
It adds a thin layer over the motor framework to vend primitive objects for use in your handler classes.

##Base Handlers 

There are several base handlers you can leverage to speed up your development.  l

- BaseHandler, base utilities for creating mongo queries from user input
- BaseMotorSearch, A simple endpoint for querying an object of a given type
- BaseRestfulMotorHandler, A conventions based handler for creating schedulable RESTful resources
- RevisionHandler, An implementation for dealing with revisions of various types
- BaseBulkScheduleableUpdateHandler, can schedule many updates or deletes to objects of a given type by passing an array of ids

##Scheduling

Requests that you want to schedule should have a "Caesium-TOA" header with a UTC
timestamp as the value for the time you would like this document to be scheduled.

Scheduled actions map to RESTful verbs: 

- POST with Caesium-TOA header would be a scheduled insert
- PUT with Caesium-TOA header is a scheduled PATCH to the object via mongo $set query
- DELETE with Caesium-TOA is a scheduled delete from the master collection
Note: *Deletes do not delete from the revisions collection for that object type, which can allow for easy reverts, but a potentially large database*

The primary way to interact with the scheduling system is to use the "Caesium-TOA" header and set a utc timestamp to the time you want your action to be performed.
