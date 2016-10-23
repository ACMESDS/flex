# FLEX

[![Forked from SourceForge](https://sourceforge.net)]

FLEX provides a CRUDE interface on FLEX[ select | update | delete | insert | execute ].table for
searching and editing a database (default MySQL cluster) that scales with the  underlying
VM architecture.  FLEX provides this capability via the following tables:

	+ dsvar	MySQL cluster tables via the [database agnosticator](https://git.geointapps.org/acmesds/dsvar)
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

FLEX also provides limited CRUDE support to the following virtual tables:
 
		table		import/export/sync of records on x=execute
		============================================================
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
 		CATALOG	catalog of tables
 		VIEWS		areas containing skinning files
 		LINKS		links to skinning files
 		SUMMARY	summarize system keep-alive info
 		USERS		status of active user sessions
 		ENGINES		status of simulation engines
 		CONFIG		system sw/hw configuration
 		TABLES		status of mysql table sources
 		ADMIN		...
 		QUEUES		status of qos-priority queues	
 		CLIQUES		cliques formed between tables and users
 		HEALTH		system health
		
## Examples

Require FELX and add interval for virtual table X are needed:

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
	
Typically, you will want to redirect the following to your project/master

	ln -s ../master/test.js test.js
	ln -s ../master/maint.sh maint.sh
	
## License

[MIT](LICENSE)
