## Introduction

This tool is used for calculating the number of occurrences of the word (w) published within a circular region of radius (r), having a central point of (long, lat), mentioned in tweets published during the time interval (start, end).

## Project Structure

We use sparkSQL in scala to manipulate the data from MongoDB and perform these steps:

- Reading the JSON-formated tweets in the attached file and using MongoSpark library to insert them into the MongoDB database in the collection
- Converting the timestamp associated with each tweet to a Date object, where the timestamp field is to be indexed
- Indexing the geo-coordinates of tweets to ensure a fast spatial-based retrieval
- Calculating the number of occurrences of word w published within a circular region of radius (r), having a central point of (long, lat), mentioned in tweets published during the time interval (start, end).

## How to use

#### to run the tool:

- run this command `WordFreqCalculator.scala PATH DATABASE COLLECTION WORD RADIUS LONG LAT START END`
  - `PATH` is the path for the JSON tweets file
  - `DATABASE` is the database name
  - `COLLECTION` is the collection name
  - `WORD` is the word to find its number of occurrences
  - `RADIUS` is the radius of the circular region where the tweets were published
  - `LONG` and LAT are the central point
  - `START` and END are time interval where the tweets were published
