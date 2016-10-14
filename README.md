# Totem SQL

[![Forked from SourceForge](https://sourceforge.net)]

[Totem](https://git.geointapps.org/acmesds/transfer)'s SQL provides a normalized JS dataset interface 
to a (default MYSQL-Cluster) database using:
	
	sql.context( {ds1:ATTRIBUTES, ds2:ATTRIBUTES, ... }, function (ctx) {

		var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;

	});

where dsX are datasets and where sql in a mysql connector.  Or, lone datasets can be created:

	var ds = SQL.DSVAR(sql, ATTRIBUTES);

where ATTRIBUTES = {key:value, ... } are described below.  In this way, dataset queries can be 
performed in a db-agnostic way using:

	ds.rec = { FIELD:VALUE, ... }				// update matched record(s) 
	ds.rec = [ {...}, {...}, ... ]						// insert record(s)
	ds.rec = null 										// delete matched record(s)
	ds.rec = function CB(recs,me) {...}		// select matched record(s)

with callback to its non-null response .res method when the query completes.  Any CRUDE 

		"select" | "delete" | "update" | "insert" | "execute" 

query can also be performed using:

	ds.res = callback() { ... }
	ds.data = [ ... ]
	ds.rec = CRUDE

or in record-locked mode using:

	ds.rec = "lock." + CRUDE

Dataset ATTRIBUTES = { key: value, ... } include:

	table: 	"DB.TABLE" || "TABLE"
	where: 	[ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, "CLAUSE":null, FIELD:[MIN,MAX], ...} | "CLAUSE"
	res: 	function (ds) {...}

	having: [ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, "CLAUSE":null, FIELD:[MIN,MAX], ...} | "CLAUSE"
	order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] | "FIELD, ..."
	group: 	[ FIELD, ...] | "FIELD, ..."
	limit: 	[ START, COUNT ] | {start:START, count:COUNT} | "START,COUNT"
	index:	[ FIELD, ... ] | "FIELD, ... " | { nlp:PATTERN, bin:PATTERN, qex: PATTERN, browse:"FIELD,...", pivot: "FIELD,..." }

	unsafeok: 	[true] | false 		to allow/block potentially unsafe CLAUSE queries
	trace: [true] | false				to display formed queries
	journal: true | [false] 			enable table journalling
	ag: "..." 								aggregate where/having with least(?,1), greatest(?,0), sum(?), ...

Null attributes are ignored.   

The select query will callback the CB=each/all/clone/trace handler with each/all record(s) matched 
by .where, indexed by  .index, ordered by .order ordering, grouped by .group, filtered by .having 
and limited by .limit ATTRIBUTEs.  Select will use its .index ATTRIBUTE to search for PATTERN 
using nlp (natural language parse), bin (binary mode), or qex (query expansion), and can browse or 
pivot the dataset.

## DEBE Extensions

SQL also provides a CRUDE interface to virtual tables.  The following tables are predefined at 
SQL[CRUDE].table and are used extensively by DEBE:
 
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

Require and config SQL:

	var SQL = require("sql").config({ ... });
	
There is nothing to configure if the default MySQL-Cluster support suffices.  SQL extends [ENUM](https://git.geointapps.org/acmesds/eum) 
so its start() options can be specified using ENUM.copy() conventions:

	options =  {
		key: value, 						// set 
		"key.key": value, 					// index and set
		"key.key.": value,					// index and append
		OBJECT: [ function (){}, ... ], 	// add prototypes
		Function: function () {} 			// add callback
		:
		:
	}

Create dataset

	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,rec:res});

Create dataset and access each record

	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,limit:[0,1],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:['x','%ll%'],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:['a',0,5],rec:function each(rec) {console.log(rec)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:"a<30",rec:function each(rec) {console.log(rec)}});		

Create dataset and access all records

	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,where:{"a<30":null,"b!=0":null,"x like '%ll%'":null,ID:5},rec:function (recs) {console.log(recs)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,order:[{property:"a",direction:"asc"}],rec:function (recs) {console.log(recs)}});
	var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,index:{pivot:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});

Select ds record(s) matched by ds.where

	ds.where = [1,2];
	ds.rec = function (rec) {
		console.log(rec);
	}

Delete ds record(s) matched by ds.where

	ds.where = {ID:2}
	ds.rec = null

Update ds record(s) matched by ds.where

	ds.where = null
	ds.rec = [{a:1,b:2,ds:"hello"},{a:10,b:20,x:"there"}]
	ds.where = {ID:3}
	ds.rec = {a:100} 
	
## Installation

Download and unzip into your project/totem folder and revise the project/config module as needed
for your [Totem](https://git.geointapps.org/acmesds/transfer) project.  Typically, you will
want to:

	ln -s project/config/maint.sh maint.sh
	
to override the defaults.
	
## License

[MIT](LICENSE)
