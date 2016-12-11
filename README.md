/**
@class flex [![Forked from SourceForge](https://sourceforge.net)]
# FLEX

FLEX provides a CRUDE (select | update | delete | insert | execute) query interface to a dataset DS on 
FLEX[ CRUD ][ DS ].  This interface supports both MySQL and emulated) datasets distributed across 
an elastic virtual machine cloud.  

FLEX uses the [DSVAR database agnosticator](https://git.geointapps.org/acmesds/dsvar) 
defaulted for a MySQL Cluster.  

FLEX provides the following emulated tables:

	+ git			local repo history, commit changes, sync with remote repo
	+ uploads	get/upload file(s) into one-time file upload area
	+ stores	get/upload file(s) into monitored file store area
	+ email		send/get SMTP email
	+ feed		send/get RSS feeds
	+ engine 	run simulation engine 
	+ catalog	read/flatten the marster catalog
 	+ json		edit a json formatted string
 	+ job		get, add, stop, and update a job placed in qos-priority queues
	+ sql		crude engines to sql tables

CRUDE-execute support is provided to the following virtual tables:
 
 		parms		 		roles			lookups				searches
 		swaps		 		news			milestones 		sockets		 		
		engines	 		collects	 		events		 		issues		
 		aspreqts	 		ispreqts		likeus
 		
and CRUDE-select suppport is provided at:

 		ACTIVITY	system activity
 		CATALOG	catalog of tables
 		VIEWS		areas containing skinning files
 		LINKS		links to skinning files
 		SUMMARY	summarize system keep-alive info
 		USERS		status of active user sessions
 		ENGINES		status of simulation engines
 		CONFIG		system sw/hw configuration
 		TABLES		status of mysql table sources
 		ADMIN		system admin data
 		QUEUES		status of qos-priority queues	
 		CLIQUES		cliques formed between tables and users
 		HEALTH		system health
		
To use, simply require FLEX and add interfaces for the virtual table X you need:

	var FLEX = require("sql").config({ 
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