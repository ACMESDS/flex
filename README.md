/**
@class FLEX
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/flex.git) 
	[geointapps](https://git.geointapps.org/acmesds/flex)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/flex.git)

FLEX provides a CRUDE interface to support [nosql-agnosictic distributed database](https://git.geointapps.org/acmesds/jsdb), 
job queueing and job agents, file uploading, dataset emulation, engine plugins, dataset schema editor, email, new feeder, 
quizzes via CRUDE emulated datasets:

	CRUDE keyedits edit plug keys
	CRUE uploads	get/upload file(s) into one-time file upload area
	CRUE stores	get/upload file(s) into monitored file store area
	RE baseline local repo history, commit changes, sync with remote repo
	RE email list / autoflush (send/get) SMTP email
	RE catalog	read/flatten the master catalog
	RE news			
	RE issues		
	RE detectors
	RE baseline
	RE tasks list/autoupdate tasks
	R likeus provide client addition credit to run jobs
	R history	allows moderators to approve requirement changes etc
	R plugins list avaliable plugins
	R activity
	R views
	R summary
	R users
	R config
	R datasets
	R admin
	R cliques
	R health
	R likeus
	R tips
	R history
	R AlgorithmService Hydra job agent
	R parms		 
	R roles			
	R lookups				
	R searches
	R swaps		 	
	E milestones 		
	E sessions	
	E catalog flatten catalog

Legacy emulated datasets:

	collects	 	
	events		 		
	json edit a json formatted string 
 	aspreqts	 	
	ispreqts		
	hawks
	engine run simulation engine 


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