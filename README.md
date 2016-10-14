# Totem SQL

[![Forked from SourceForge](https://sourceforge.net)]

[Totem](https://git.geointapps.org/acmesds/transfer)'s SQL module provides a normalized CRUDE
(x=select | update | insert | delete | execute) interface to MYSQL-Cluster tables on SQL[x].ds, and 
to the following virtual tables on SQL[x].table
 
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

	This SQL module also provides a means to encapsulate its underlying (default MySQL) 
	database table into a database agnostic JS dataset using the following mechanisim:
	
		var ds = SQL.DSVAR(sql, { ATTRIBUTE: VALUE, ... })
	
	where its ATTRIBUTEs are:
	
		table: 	"DB.TABLE" || "TABLE"
		where: 	[ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, "CLAUSE":null, FIELD:[MIN,MAX], ...} | "CLAUSE"
		res: 	function (ds) {...}

		having: [ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, "CLAUSE":null, FIELD:[MIN,MAX], ...} | "CLAUSE"
		order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] | "FIELD, ..."
		group: 	[ FIELD, ...] | "FIELD, ..."
		limit: 	[ START, COUNT ] | {start:START, count:COUNT} | "START,COUNT"
		index:	[ FIELD, ... ] | "FIELD, ... " | { nlp:PATTERN, bin:PATTERN, qex: PATTERN, browse:"FIELD,...", pivot: "FIELD,..." }

	Null attributes are ignored.   In this way, dataset queries can be performed in a db-agnostic way using:
	
		ds.rec = { FIELD:VALUE, ... }			// update matched record(s) 
		ds.rec = [ {...}, {...}, ... ]			// insert record(s)
		ds.rec = null 							// delete matched record(s)
		ds.rec = function CB(recs,me) {...}		// select matched record(s)
		
	with callback to its non-null response .res method when the query completes.  A CRUDE = 
	"select" | "delete" | "update" | "insert" | "execute" query can also be performed using:
	
		ds.res = callback() { ... }
		ds.data = [ ... ]
		ds.rec = CRUDE
		
	or in record-locked mode using:
	
		ds.rec = "lock." + CRUDE
	
	The select query will callback the CB=each/all/clone/trace handler with each/all record(s) matched 
	by .where, indexed by  .index, ordered by .order ordering, grouped by .group, filtered by .having 
	and limited by .limit ATTRIBUTEs.  Select will use its .index ATTRIBUTE to search for PATTERN 
	using nlp (natural language parse), bin (binary mode), or qex (query expansion), and can browse or 
	pivot the dataset.

	Additional dataset ATTRIBUTEs:
		
		attr	default	true/set to
		-----------------------------------------------------------------------------------
		unsafeok 	true	execute potentially unsafe CLAUSE queries
		trace	false	display formed queries
		journal	false	enable table journalling
		ag		null	aggregate where/having = least(?,1), greatest(?,0), sum(?), ...
		
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
		var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,index:{pivot:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});
		
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
