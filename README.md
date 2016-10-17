# Totem SQL

[![Forked from SourceForge](https://sourceforge.net)]

[Totem](https://git.geointapps.org/acmesds/transfer)'s SQL provides a CRUDE interface to the following virtual tables.
 
  		table		functionality provided on x=all
 		=======================================================================
 		json		edit a json formatted string
 		jobs		get, add, stop, and update a job placed in qos-priority queues
 		sql			crude engines to sql tables
 		git			get local repo history, commit changes to local repo, sync with remote repo
 		uploads	one-time file upload area
 		stores	monitored file store area
 		
		table		import/export/sync of records on x=execute
		==============================================================================
 		parms		
 		roles		
 		lookups		
 		searches
 		swaps		
 		news		
 		milestones	
 		sockets		
 		engines	
 		collects	
 		events		
 		issues		
 		aspreqts	
 		ispreqts	
 		
		table		information returned on x=select
		======================================================================
		likeus	gives us a like
 		tips	...
 		ACTIVITY	system activity
 		CATALOG		catalog of tables
 		VIEWS		areas containing skinning files
 		LINKS		links to skinning files
 		SUMMARY		summarize system keep-alive info
 		USERS		status of active user sessions
 		ENGINES		status of simulation engines
 		CONFIG		system sw/hw configuration
 		TABLES		status of mysql table sources
 		ADMIN		...
 		QUEUES		status of qos-priority queues	
 		CLIQUES		cliques formed between tables and users
 		HEALTH		system health
		
## Examples

Require and, if alternate virtuale tables X are needed, config SQL:

	var SQL = require("sql").config({ 
		select: { X: function (req,res), ... },
		delete: { X: function (req,res), ... },
		update: { X: function (req,res), ... },
		insert: { X: function (req,res), ... },
		execute: { X: function (req,res), ... }
	});
	
There is nothing to configure if the default MySQL-Cluster support suffices. 

## Installation

Download the latest version with

	git clone https://git.geointapps.org/acmesds/sql
	
See [Totem downloads](https://git.geointapps.org/acmesds/download) for optional Totem plugins.

## License

[MIT](LICENSE)
