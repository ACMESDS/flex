# Totem SQL

[![Forked from SourceForge](https://sourceforge.net)]

The [Totem](https://git.geointapps.org/acmesds/transfer) SQL module provides a normalized 
CRUDE (x=select | update | insert | delete | execute) interface to its underlying 
(default MySQL-Cluster) databse on SQL[x].ds, and to the following virtual
tables on SQL[x].table where

	table		functionality provided on x=all
	=======================================================================
	json		edit a json formatted string
	jobs		get, add, stop, and update a job placed in qos-priority queues
	sql			crude engines to sql tables
	git			get local repo history, commit changes to local repo, sync with remote repo
	uploads
	stores
	
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
	likeus
	tips
	ACTIVITY	
	CATALOG		catalog of tables
	VIEWS		areas containing skinning files
	LINKS		links to skinning files
	SUMMARY		summarize system keep-alive info
	USERS		status of active user sessions
	ENGINES		status of simulation engines
	CONFIG		system sw/hw configuration
	TABLES		status of mysql table sources
	ADMIN		
	QUEUES		status of qos-priority queues	
	CLIQUES		cliques formed between tables and users
	HEALTH		system health

Totem's SQL module also provides a means to encapsulate its underlying (default MySQL-Cluster) 
database into database-agnostic JS datasets as explained in the Usage notes.

## Installation

Download and unzip into your project/sql folder and revise the project/config module as needed
for your [Totem](https://git.geointapps.org/acmesds/transfer) project.  Typically, you will
want to:

	ln -s project/config/debe.sh config.sh
	ln -s project/config/maint.sh maint.sh
	ln -s project/config/certs certs
	
to override the defaults.

## Usage

This SQL module provides a means to encapsulate its underlying (default MySQL-Cluster) 
database database-agnostic JS-datasets using the following mechanisim:

	var ds = SQL.DSVAR(sql, { KEY: VALUE, ... })

where its KEY attributes are:

	table: 	"DB.TABLE" || "TABLE"
	where: 	[ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} | "CLAUSE"
	having: [ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} | "CLAUSE"
	order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] | "FIELD, ..."
	group: 	[ FIELD, ...] | "FIELD, ..."
	limit: 	[ START, COUNT ] | {start:START, count:COUNT} | "START,COUNT"
	index:	[ FIELD, ... ] | "FIELD, ... "
	res: 	function (ds) {...}

Null attributes are ignored during queries.  Queries to dataset records can then be 
performed in a db-agnostic way using:

	ds.rec = { FIELD:VALUE, ... }			// update matched record(s) 
	ds.rec = [ {...}, {...}, ... ]			// insert record(s)
	ds.rec = null 							// delete matched record(s)
	ds.rec = function F(recs,me) {...}		// select matched record(s)
	
and will callback its non-null response .res method when the query completes.  

The select query will callback the F=each/all/clone handler with all record(s)
matched by .where, indexed by  .index, ordered by .order ordering, grouped by 
.group, filtered by .having and limited by .limit attributes.  The select query
will smartly group-browse or group-pivot records if its .where contains a 
BrowseID or a NodeID, respectively.

CRUDE = "select" | "delete" | "update" | "insert" | "execute" queries can 
also be performed using:

	ds.res = callback() { ... }
	ds.data = [ ... ]
	ds.rec = CRUDE
	
or in record-locked mode using:

	ds.rec = "lock." + CRUDE

Additional dataset attributes:
	
	attr	default	true/set to
	-----------------------------------------------------------------------------------
	unsafe 	true	execute potentially unsafe queries
	trace	false	display formed queries
	journal	false	enable table journalling
	ag		null	aggregate where/having = least(?,1), greatest(?,0), sum(?), ...
	nlp		null	search pattern using natural language parse
	bin		null	search pattern in binary mode
	qex		null	search pattern with query expansion
	
A context of datasets can be established on the same sql connector with:

	sql.context( {ds1:{attributes}, ds2:{attributes}, ... }, function (ctx) {
	
		var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;
	
	});
			
## Examples

	// create dataset
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,rec:res});
	
	// create dataset and access each record
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,limit:[0,1],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:['x','%ll%'],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:['a',0,5],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:"a<30",rec:function each(rec) {console.log(rec)}});		
	
	// create dataset and access all records

	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:{"a<30":null,"b!=0":null,"x like '%ll%'":null,ID:5},rec:function (recs) {console.log(recs)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,order:[{property:"a",direction:"asc"}],rec:function (recs) {console.log(recs)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,index:{NodeID:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});
	
	// select ds record(s) matched by ds.where
	ds.where = [1,2];
	ds.rec = function (rec) {
		console.log(rec);
	}

	// delete ds record(s) matched by ds.where
	ds.where = {ID:2}
	ds.rec = null
	
	// update ds record(s) matched by ds.where
	ds.where = null
	ds.rec = [{a:1,b:2,ds:"hello"},{a:10,b:20,x:"there"}]
	ds.where = {ID:3}
	ds.rec = {a:100} 
	
## License

[MIT](LICENSE)
