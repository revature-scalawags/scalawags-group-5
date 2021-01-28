# Batch-Data-Init

## Project Description
---
Takes a large file from containing [old twitter data](https://archive.org/search.php?query=collection%3Atwitterstream&sort=-publicdate) and formats the data to have a structured format.  

<br>

## Technologies Used
---
* scala - version 2.13.4
* sbt - version 1.4.4

<br>

## Features
---
List of features ready
* Read in and reformat many Json files to a singular file

<br>

## Getting Started
---
- Clone this repository with the following command:
```bash
git clone https://github.com/revature-scalawags/scalawags-group-5.git
```
- Download one file from: https://archive.org/search.php?query=collection%3Atwitterstream&sort=-publicdate&page=2
- Example: https://archive.org/download/archiveteam-twitter-stream-2020-03/twitter_stream_2020_03_01.tar

Move file locally to batch-data-init file
Unzip file and all sub folders - only need all files in an hour or one minute in every hour.

Change script file to be pointing to this folder.

<br>

## Usage
---

* Make sure docker is running

> ```sh run.sh```
