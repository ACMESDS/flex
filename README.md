/**
@class FLEX
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/flex.git) 
	[geointapps](https://git.geointapps.org/acmesds/flex)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/flex.git)

FLEX provides a CRUDE interface to MYSQL datasets, job queues, file upload areas, emulated datasets,  
engine plugins, dataset schema editor, email, new feeder, job agents, quizzes, and network services distributed 
across a virtual machine cloud.  

FLEX uses the [JSDB database agnosticator](https://git.geointapps.org/acmesds/jsdb) 
for its MYSQL datasets, and provides the following emulated datasets:

	baseline		local repo history, commit changes, sync with remote repo
	uploads	get/upload file(s) into one-time file upload area
	stores	get/upload file(s) into monitored file store area
	email		send/get SMTP email
	feed		send/get RSS feeds
	engine 	run simulation engine 
	catalog	read/flatten the master catalog
 	json		edit a json formatted string
 	job		get, add, stop, and update a job placed in qos-priority queues
	sql		crude engines to sql datasets
	likeus 	provide client some credit
	history	moderator history
	plugins	list of plugins
	tasks	list of tasks

FLEX also adds a CRUDE-execute interface to the following datasets:
 
 		parms		 	roles			lookups				searches
 		swaps		 	news			milestones 		sessions	
		collects	 	events		 		issues		detectors
 		aspreqts	 	ispreqts		hawks
 		
To use, simply require FLEX and add interfaces for the virtual table X:

	var FLEX = require("sql").config({   // CRUDE interface
		select: { X: function (req,res), ... },
		delete: { X: function (req,res), ... },
		update: { X: function (req,res), ... },
		insert: { X: function (req,res), ... },
		execute: { X: function (req,res), ... }
	});
	
There is nothing to configure if the default MySQL-Cluster support suffices. 

## Installation

Download the latest version with

	git clone https://git.geointapps.org/acmesds/flex
	
## License

[MIT](LICENSE)

*/