#Ceasium

Caesium is a document scheduling tool built on top of MongoDB, Tornado, and Motor frameworks.

- Schedule create, update and delete actions at UTC timestamp
- Revisions are kept in separate collections per resource type
- A useful document ORM is provided that vends dictionaries with BSON types properly adapted
- Abstracts the use of Cursor objects to document ORM
- Uses coroutines by default and won't block unless you call yield in your handlers

##Build Status

![Travis Build](https://travis-ci.org/urbn/Caesium.svg?branch=master)