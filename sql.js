// UNCLASSIFIED $$$$

/**
 * @module SQL
 * nodejs:
 * @requires os
 * @requires fs
 * @requires cluster
 * @requires child_process
 * @requires vm
 * @requires http
 * @requires crypto
 * @requires url
 * totem:
 * @requires enum
 * @requires engine
 * 3rd party:
 * @requires pdffiller
 * @requires mysql
 * @requires nodemailer
 * @requires imap
 * @requires graceful-lwip
 * @requires feed
 * @requires feed-read
 */
/**
	SQL provides a normalized CRUDE (x=select | update | insert | delete | execute) 
	interface to MYSQL-Cluster tables on SQL[x].ds, and to the following virtual
	tables on SQL[x].table
 
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

	This SQL module also provides a means to encapsulate its underlying (default MySQL) 
	database table into a database agnostic JS dataset using the following mechanisim:
	
		var ds = SQL.DSVAR(sql, { ATTRIBUTE: VALUE, ... })
	
	where its ATTRIBUTEs are:
	
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
		
	and will callback its non-null response .res method when the querey completes.  
	
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
		unsafeok 	true	execute potentially unsafe queries
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
				
	Examples:
	
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
	
*/
 
var 									// nodejs bindings
	VM = require('vm'), 				// V8 JS compiler
	CLUSTER = require('cluster'),		// Support for multiple cores	
	HTTP = require('http'),				// HTTP interface
	CRYPTO = require('crypto'),			// Crypto interface
	//NET = require('net'), 				// Network interface
	URL = require('url'), 				// Network interface
	FS = require('fs'),					// Need filesystem for crawling directory
	CP = require('child_process'), 		// Child process threads
	OS = require('os');					// OS utilitites

var 									// 3rd party bindings
	//PDF = require('pdffiller'), 		// pdf form processing
	MAIL = require('nodemailer'),		// MAIL mail sender
	SMTP = require('nodemailer-smtp-transport'),
	IMAP = require('imap'),				// IMAP mail receiver
	ENGINE = require("engine"), 		// tauif simulation engines
	FEED = require('feed');				// RSS / ATOM news feeder
	//READ = require('feed-read'); 		// RSS / ATOM news reader

var 											// globals
	ENV = process.env, 							// external variables
	LIST = ",",									// list separator
	DOT = ".", 									// table.type etc separator
	SLASH = "/",
	SUBMITTED = "submitted";
	//LOCKS = {};									// database locks

var 											// nodejs bindings
	FS = require("fs"), 						// file system resources
	OS = require("os"), 						// operating system resources
	CP = require("child_process"), 				// Process spawning
	CLUSTER = require('cluster'); 				// Work cluster manager

var												// 3rd party bindings
	MYSQL = require("mysql");

var 											// totem bindings
	ENUM = require("enum").extend({
		Array: [
			function escape() {
				var q = "`";
				
				if (this)
					return (q+this.join(`${q},${q}`)+q).split(",").join(",");
					
				else
					return "";
			}
		]
	}),
	Copy = ENUM.copy,
	Each = ENUM.each;

function Trace(msg,arg) {
	
	if (msg.constructor == String)
		console.log("S>"+msg);
	else
		console.log("S>"+msg.sql);

	if (arg) console.log(arg);
		
	return msg;
}

var
	SQL = module.exports = {
		
		// Job hawking  etc
		timers: [],
		sendMail: sendMail,
		
		errors: {
			unsafeQuery: new Error("unsafe queries not allowed"),
			unsupportedQuery: new Error("query not supported"),
			invalidQuery: new Error("query invalid"),
			disableEngine: new Error("requested engine must be disabled to prime"),
			noEngine: new Error("requested engine does not exist"),
			missingEngine: new Error("missing engine query"),
			protectedQueue: new Error("action not allowed on this job queues")
		},
		
		listify: function (hash, idxkey, valkey) {
			var list = [];
			var n = 0;
			
			if (idxkey)
				for (var idx in hash) {
					rec = hash[idx];
					rec[idxkey] = n++;
					list.push( rec );
				}	
			else
				for (var idx in hash) 
					list.push(idx);
					
			return list;
		},
		
		// CRUDE interface
		select: {ds: crude}, 
		delete: {ds: crude}, 
		update: {ds: crude}, 
		insert: {ds: crude}, 
		execute: {ds: crude}, 
	
		RECID: "ID", 					// Default unique record identifier
		emit: null,		 				// Emitter to sync clients
		thread: null, 					// SQL connection threader
		skin : null, 					// Jade renderer
		TRACE : true,					// Trace SQL querys to the console
		//BIT : null,					// No BIT-mode until set to a SYNC hash
		//POOL : null, 					// No pool until SQB configured
		//DB: "none", 					// Default database
		//SQL : null,	 				// Default virtual table logic
		//USER : ENV.DB_USER,			// SQL client account (safe login for production)
		//PASS : ENV.DB_PASS,			// Passphrase to the SQL DB 
		//SESSIONS : 2,					// Maxmimum number of simultaneous sessions
		DBTX : {						// Table to database translator
			issues: "openv.issues",
			tta: "openv.tta",
			standards: "openv.standards",
			milestones: "openv.milestones",
			txstatus: "openv.txstatus",
			apps: "openv.apps",
			profiles: "openv.profiles",
			trades: "openv.trades",
			hwreqts: "openv.hwreqts",
			oplimits: "openv.oplimits",
			swreqts: "openv.swreqts",
			FAQs: "openv.FAQs",
			aspreqts: "openv.aspreqts",
			ispreqts: "openv.ispreqts" 
		},
		RESET : 12,	 					// mysql connection pool timer (hours)
		RECID : "ID",					// DB key field
		NODENAV : {						// specs for folder navigation
			ROOT : "", 					// Root node ID
			JOIN : ",',',"				// Node joiner (embed same slash value above)
		},
		DEFTYPES : {
			"#": "varchar(32)",
			"_": "varchar(1)",
			a: "float unique auto_increment",
			t: "varchar(64)",
			n: "float",
			x: "mediumtext",
			h: "mediumtext",
			i: "int(11)",
			c: "int(11)",
			d: "date",
			f: "varchar(255)"
		},
		
		/**
		 * @method config
		 * 
		 * Configure module with spcified options, then callback the
		 * initializer.  
		 * */
		config: function (opts) {
			
			if (opts) Copy(opts,SQL);
			
			if (SQL.thread)
			SQL.thread( function (sql) {
				
				Trace("EXTENDING SQL CONNECTOR");
				
				ENUM.extend(sql.constructor, [
					selectJobs,
					deleteJobs,
					updateJobs,
					insertJobs,
					executeJobs,
					hawkCatalog,
					context,
					crude, 
					flattenCatalog
				]);

				sql.release();
			});
			
			var
				SITE = SQL.SITE;
				EMAIL = SQL.EMAIL;
				
			if (CLUSTER.isMaster) {
				
				// setup news feeder
				
				NEWSFEED = new FEED({					// Establish news feeder
					title:          SITE.nick,
					description:    SITE.title,
					link:           `${SQL.URL.HOST}/feed.view`,
					image:          'http://example.com/image.png',
					copyright:      'All rights reserved 2013',
					author: {
						name:       "tbd",
						email:      "noreply@garbage.com",
						link:       "tbd"
					}
				});

				// setup likeus table hawk
				if (SQL.LIKEUS.PING)
					setInterval( function() {
						
						SQL.thread(function (sql) {
							
							console.log("PROFILES UPDATED");
							
							sql.query(
								"SELECT *, datediff(now(),Updated) AS Age FROM openv.profiles WHERE LikeUs HAVING Age>=?",
								[SQL.LIKEUS.BILLING]
							)
							.on("result", function (rec) {		
								sql.query("UPDATE openv.profiles SET LikeUs=LikeUs-1 WHERE ?",{ID:rec.ID} );
							})
							.on("end",sql.release);
							
						});
						
					}, SQL.LIKEUS.PING*(3600*24*1000) );

				/*
				if (opts.PULSE.PING)
					setInterval( function() {
						
						SQL.thread(function (sql) {

							sql.query("SELECT count(ID) AS Count FROM engines WHERE Enabled")
							.on("result", function (engs) {
							sql.query("SELECT count(ID) AS Count FROM queues WHERE Departed IS NULL")
							.on("result", function (jobs) {
							sql.query("SELECT sum(DateDiff(Departed,Arrived)>1) AS Count from queues")
							.on("result", function (pigs) {
							sql.query("SELECT sum(Delay>20)+sum(Fault != '') AS Count FROM dblogs")
							.on("result", function (isps) {
								var rtn = SQL.PULSE.COUNTS = {Engines:engs.Count,Jobs:jobs.Count,Pigs:pigs.Count,Faults:isps.Count,State:"ok"};
								var lims = SQL.PULSE.LIMITS;
								
								for (var n in lims) 
									if ( rtn[n] > 5*lims[n] ) rtn.State = "critical";
									else
									if ( rtn[n] > lims[n] ) rtn.State = "warning";
									
								console.log("SYSTEM "+rtn.State);
							});
							});
							});
							});
						});
						
					}, opts.PULSE.PING*(60*1000) );
				*/
				
				//$$$$
				EMAIL.HAWKS = SITE.distro.hawk; 
				EMAIL.SUBJ = SITE.nick + "?JOB";
				EMAIL.BODY = 
					("Click " 
					+ "here".tag("a",{href:"?URLtipclear.db?ID=?JOBID"})
					+ " to clear this tip."
					).tag("p").tag("html");

				var parts = (SITE.emailhost||"").split(":");
				EMAIL.TX.HOST = parts[0];
				EMAIL.TX.PORT = parseInt(parts[1]);
				
				var parts = (SITE.emailuser||"").split(":");
				EMAIL.USER = parts[0];
				EMAIL.PASS = parts[1];
				
				if (EMAIL.TX.PORT) {  		// Establish server's email transport
					
					console.log("MAILING  "+EMAIL.TX.HOST+":"+EMAIL.TX.PORT + " AS " + EMAIL.USER);
					
					EMAIL.TX.TRAN = EMAIL.USER
						? 
						MAIL.createTransport({ //"SMTP",{
							host: EMAIL.TX.HOST,
							port: EMAIL.TX.PORT,
							auth: {
								user: EMAIL.USER,
								pass: EMAIL.PASS
							}
						})
						: MAIL.createTransport({ //"SMTP",{
							host: EMAIL.TX.HOST,
							port: EMAIL.TX.PORT
						});
						
				}
							
				if (EMAIL.RX.PORT)
					EMAIL.RX.TRAN = new IMAP({
						  user: EMAIL.USER,
						  password: EMAIL.PASS,
						  host: EMAIL.RX.HOST,
						  port: EMAIL.RX.PORT,
						  secure: true,
						  //debug: function (err) { console.warn(ME+">"+err); } ,
						  connTimeout: 10000
						});

				if (EMAIL.RX.TRAN)					// Establish server's email inbox			
					openIMAP(function(err, mailbox) {
					  if (err) killIMAP(err);
					  EMAIL.RX.TRAN.search([ 'UNSEEN', ['SINCE', 'May 20, 2012'] ], function(err, results) {
						if (err) killIMAP(err);
						EMAIL.RX.TRAN.fetch(results,
						  { headers: ['from', 'to', 'subject', 'date'],
							cb: function(fetch) {
							  fetch.on('message', function(msg) {
								console.info(ME+'Saw message no. ' + msg.seqno);
								msg.on('headers', function(hdrs) {
								  console.info(ME+'Headers for no. ' + msg.seqno + ': ' + showIMAP(hdrs));
								});
								msg.on('end', function() {
								  console.info(ME+'Finished message no. ' + msg.seqno);
								});
							  });
							}
						  }, function(err) {
							if (err) throw err;
							console.info(ME+'Done fetching all messages!');
							EMAIL.RX.TRAN.logout();
						  }
						);
					  });
					});
				
				if (SQL.EMAIL.ONSTART) 
					sendMail({
						from: SITE.nick, 
						to: EMAIL.HAWKS,
						subject: SITE.title + " started", 
						html: "Just FYI",
						alternatives: [{
							contentType: 'text/html; charset="ISO-59-1"',
							contents: ""
						}]
					}, function (err,info) {
						if (err) console.log(`EMAIL ${err}`);
					});
			}			
		}

};

/**
 * @method flattenCatalog
 * 
 * Flatten entire database for searching the catalog
 * */
function flattenCatalog(flags, catalog, limits, cb) {
	
	function flatten( sql, rtns, depth, order, catalog, limits, returncb, matchcb) {
		var table = order[depth];
		
		if (table) {
			var match = catalog[table];
			var selects = matchcb(match);
			
			var qual = " using "+ (selects ? selects : "open")  + " search limit " + limits.records;
			
			Trace("CATALOG ("+table+qual+") HAS "+rtns.length);
		
			var query = selects 
					? "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID, " + selects + " FROM ?? HAVING Score>? LIMIT 0,?"
					: "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID FROM ?? LIMIT 0,?";
					
			var args = selects
					? [table, limits.score, limits.records]
					: [table, limits.records];

			sql.query( query, args,  function (err,recs) {
				
				if (err) {
					rtns.push( {
						ID: rtns.length,
						Ref: table,
						Name: "error",
						Dated: limits.stamp,
						Searched: 0,
						Link: (table + ".db").tag("a",{href: "/" + table + ".db"}),
						Content: err+""
					} );

					flatten( sql, rtns, depth+1, order, catalog, limits, returncb, matchcb );
				}
				else 
					sql.query("select found_rows()")
					.on('result', function (stat) {
						
						recs.each( function (n,rec) {						
							rtns.push( {
								ID: rtns.length,
								Ref: table,
								Name: table+DOT+rec.ID,
								Dated: limits.stamp,
								Searched: recs.length + " of " + stat["found_rows()"] + qual,
								Link: table.tag("a",{href: "/" + table + ".db?ID=" + rec.ID}),
								Content: JSON.stringify( rec )
							} );
						});

						flatten( sql, rtns, depth+1, order, catalog, limits, returncb, matchcb );
					});
			});	
		}
		else
			returncb(rtns);
	}

	/*
	function escape(n,arg) { return "`"+arg+"`"; }
	*/
	
	var sql = this,
		rtns = [],
		limits = {
			records: 100,
			stamp: new Date()
			//pivots: flags._pivot || ""
		};
		
	flatten( sql, rtns, 0, SQL.listify(catalog), catalog, limits, cb, function (search) {

		return Builds( "", search, flags);

	});
}

function hawkCatalog(req,res) {
	var sql = this,
		flags = req.flags;
	
	function queueExe(cls,name,exe) {
		
		sql.insertJobs({
			class: cls,
			client: req.client,
			qos: req.profile.QoS,
			priority: 0,
			key: name,
			req: Copy(req,{}),
			name: "new "+name
		}, exe);
	}
	
	Trace("HAWK CATALOG FOR "+req.client+" find="+flags.has);
	
	if (has = flags.has)
		queueExe("detector", has, function (req,res) {
			//console.log("create detector "+req.has);
			res("See "+"jobs queue".tag("a",{href:"/jobs.view"}));  
		});
		
	/*
		sql.query("INSERT INTO queues SET ? ON DUPLICATE KEY UPDATE Count=Count+1", {
			Made: new Date(),
			Searching: find.replace(/ /g,""),
			Tokens: req.table + DOT + req.flags.search,
			Returned: recs.length,
			Count: 1,
			Client: req.client
		});*/
}

/**
 * Job queue interface.
 * 
 * select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
 * execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
 * update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
 * delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
 * insert(job,cb): add job and route to callback cb(job) when executed.
 * */

SQL.queues = {};
	
/*
 * callsback cb(rec) for each queuing rec matching the where clause.
 */
 
function selectJobs(where, cb) { 

	// route valid jobs matching sql-where clause to its assigned callback cb(req).
	var sql = this;
	
	sql.query(
		where
		? `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client WHERE ${where} ORDER BY QoS,Priority`
		: `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client ORDER BY QoS,Priority`
	)
	.on("error", function (err) {
		console.log(err);
	})
	.on("result", function (rec) {
		cb(rec);
	});	
}

function updateJobs(req, cb) { 
	// adjust priority of jobs matching sql-where clause and route to callback cb(req) when updated.
	
	var sql = this;
	
	sql.selectJobs(req, function (job) {
		
		cb(job.req, function (ack) {

			if (req.qos)
				sql.query("UPDATE queues SET ? WHERE ?", [{
					QoS: req.qos,
					Notes: ack}, {ID:job.ID}]);
			else
			if (req.inc)
				sql.query("UPDATE queues SET ?,Priority=max(0,min(5,Priority+?)) WHERE ?", [{
					Notes: ack}, req.inc, {ID:job.ID}]);
			
			if (req.qos) {  // move req to another qos queue
				delete SQL.queues[job.qos].batch[job.ID];
				job.qos = req.qos;
				SQL.queues[qos].batch[job.ID] = job;
			}
			
			if (req.pid)
				CP.exec(`renice ${req.inc} -p ${job.pid}`);				
				
		});
	});
}
		
function deleteJobs(req, cb) { 
	
	var sql = this;
	sql.selectJobs(req, function (job) {
		
		cb(sql,job, function (ack) {
			sql.query("UPDATE queues SET ? WHERE ?", [{
				Departed: new Date(),
				RunTime: (new Date() - rec.Arrived)/3.6e6,
				Util: util(),
				Notes:"stopped"}, {Name:job.Name}]);

			delete SQL.queues[job.qos].batch[job.ID];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

/*
 * Adds job to requested job.qos, job.priority queue and updates its
 * associated queue record by its unique job.name.  Executes the
 * supplied callsback cb(job) with the next job ready for service, or
 * spawns the job if job.cmd provided.
 */

function insertJobs(job, cb) { 
	function util() {				// compute cpu utils and return avg util
		var avgUtil = 0;
		var cpus = OS.cpus();
		
		cpus.each(function (n,cpu) {
			idle = cpu.times.idle;
			busy = cpu.times.nice + cpu.times.sys + cpu.times.irq + cpu.times.user;
			avgUtil += busy / (busy + idle);
		});
		return avgUtil / cpus.length;
	}
	
	function regulate(job,cb) {		// regulate job and spawn if job.cmd provided
			
		var queue = SQL.queues[job.qos];
		
		if (!queue)
			queue = SQL.queues[job.qos] = {
				timer: 0,
				batch: {},
				rate: 2e3*(10-job.qos)
			};
			
		if (queue.rate > 0) { 				// regulated job
			var batch = queue.batch[job.priority];
			if (!batch) batch = queue.batch[job.priority] = [];
			
			batch.push( Copy(job, {cb:cb}) );
			
			if (!queue.timer) 		// restart idle queue
				queue.timer = setInterval(function (queue) {
					
					var job = null;
					for (var batch in queue.batch) {
						job = batch.pop();
						
						if (job) {
//console.log("job depth="+batch.length+" job="+[job.name,job.qos]);

							if (job.cmd)	// spawn job and return its pid
								job.pid = CP.exec(job.cmd, {cwd: "./public/dets", env:process.env}, function (err,stdout,stderr) {
									Trace(err + stdout + stderr);
									
									if (job.cb)
										SQL.thread( function (sql) {
											job.cb( err ? err + stdout + stderr : null );
										});
								});
							else  			// execute job cb on new sql thread
							if (job.cb) 
								SQL.thread( function (sql) {
									job.cb(sql,job);
								});
						
							break;
						}
					}
						
					if (!job) { 	// empty queue goes idle
						clearInterval(queue.timer);
						queue.timer = null;
					}

				}, queue.rate, queue);
				
			return true;
		}
		else 						// unregulated job
			return false;
	}

	var sql = this,
		jobID = {Name:job.name};
	
	var
		regulated = regulate(job, function (sql,job) {
				
			cb(sql, job);
			
			sql.query(
				"UPDATE queues SET ?,Age=datediff(now(),Arrived),Done=Done+1,State=Done/Work*100 WHERE ?", [
				{Util: util()}, jobID 
			]);
			
			sql.query(
				"UPDATE queues SET Departed=now(), Notes='finished' WHERE least(?,Done=Work)", 
				jobID );

		});

	if (regulated)
		sql.query(
			"UPDATE queues SET ?,Age=datediff(now(),Arrived),Work=Work+1,State=Done/Work*100 WHERE ?", [
			{Util:util()},
			{Name:job.name} ], 
			function (err) {
				
			if (err)
				sql.query("INSERT INTO queues SET ?", {
					Client	: job.client,
					Class	: job.class,
					State	: 0,
					Arrived	: new Date(),
					Departed: null,
					Mark	: 0,
					Job		: job.name,
					RunTime	: 0,
					Classif : "",
					Util	: util(),
					Priority: 1,
					Notes	: "running",
					QoS		: job.qos,
					Work 	: 1
				});
		});
		
	else	
		cb(sql,job);
}
	
function executeJobs(req, exe) {
}

/*
 * SQL dataset interface
 */
SQL.DSVAR = function(sql,atts,defs) {
	
	this.sql = sql;
	this.err = null;
	this.query = "";
	this.opts = null;
	this.unsafeok = true;
	this.trace = true;
	
	for (var n in defs) 
		switch (n) {
			case "select":
			case "update":
			case "delete":
			case "insert":
				this.prototype[n] = defs[n];
				break;
				
			default:	
				this[n] = defs[n];
		}
		
	if (atts.constructor == String) atts = {table:atts};
	
	for (var n in atts) this[n] = atts[n];
}

SQL.DSVAR.prototype = {
	
	x: function xquery(opt,key,buf) {  // extends me.query and me.opts
		
		function nodeify(list) {
			return "cast(concat(`" + list.join("`,',',`") + "`) AS CHAR)";
		}
		
		var me = this,
			keys = key.split(" "),
			ag = keys[1] || "least(?,1)";
			
		if (opt) 
			switch (keys[0]) {
				case "":
				case "IN":
				case "WITH":
				
					me.query += `,MATCH(FullSearch) AGAINST('${opt}' ${key}) AS Score`;
					me.having = me.score ? "Score>"+me.score : "Score";
					break;
				
				case "SELECT":

					var where = me.where;
					
					if (me.group)
					
						if (where.BrowseID) { 	// browsing a table
							var	nodes = where.BrowseID.split(","),
								browse = me.browse;
							
							me.group = browse[nodes.length];
							delete where.BrowseID;
							
							if (me.group) { 
								var pivots = nodeify(browse.slice(0,nodes.length+1));
								
								me.query += ` ${key} *`;
								me.query += `,cast(concat(${pivots}) AS CHAR) AS BrowseID`;
								me.query += ",count(ID) AS NodeCount";
								me.query += ",false AS leaf, true AS expandable, false AS expanded";
									
								nodes.each( function (n,node) {
									me.where[ browse[n] ] = node;
								});
							}
							else  {
								var pivots = nodeify(browse.concat(["ID"]));
								
								me.query += ` ${key} *`;
								me.query += `,cast(concat(${pivots}) AS CHAR) AS BrowseID`;
								me.query += ",1 as NodeCount";
								me.query += ",true AS leaf, false AS expandable, true AS expanded";
									
								nodes.each( function (n,node) {
									me.where[(n >= browse.length) ? "ID" : browse[n]] = node;
								});
							}
						}
						
						else
						if (where.NodeID) { 		// pivoting a table
							var	nodes = (where.NodeID == "root") ? [] : where.NodeID.split(",");									
							var node = nodes.length ? "" : nodeify(me.group.split(",'"));
							delete where.NodeID;
							
							me.query += node 
							 	// at the root
								? ` ${key} ${me.group}`    
									+ `,${node} AS NodeID`
									+ ",count(ID) AS NodeCount"
									+ ",false AS leaf,true AS expandable,false AS expanded"
								
								// requesting all nodes under a specific node
								:  ` ${key} `
									+ "ID AS NodeID"
									+ ",1 AS NodeCount"
									+ ",true AS leaf,true AS expandable,false AS expanded";
						}
						
						else {
							me.query += ` ${key} ${opt}`;
							me.opts.push( opt.index.split(",") );
						}
					
					else
						switch (opt.constructor) {
							case Array:
								me.query += ` ${key} ??`;
								me.opts.push(opt);
								break;
								
							case String:
								if (opt == "*") 
									me.query += ` ${key} *`;
								else {
									me.query += ` ${key} ??`;
									me.opts.push(opt.split(","));
								}
								break;
														
							case Object:
								me.query += ` ${key} *`;
								x(opt.nlp, "");
								x(opt.bin, "IN BINARY MODE");
								x(opt.qex, "WITH QUERY EXPANSION");
								break;
						}
					
					break;

				case "JOIN":
					switch (opt.constructor) {
						case Array:
							me.query += ` ${mode} ${key} ON ?`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${mode} ${key} ON ${opt}`;
							break;

						case Object:

							var mode = opt.left ? "left" : opt.right ? "right" : "";

							me.query += ` ${mode} ${key} ? ON least(?,1)`;
							
							for (var n in opt.on) 
								buf[n] = me.table+"."+opt.on[n];
								
							me.opts.push(opt[mode]);
							me.opts.push(buf);
							break;
					}
					break;
				
				case "LIMIT":
					switch (opt.constructor) {
						case Array:
							me.query += ` ${key} ?`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${key} ?`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							me.query += ` ${key} ?`;
							me.opts.push([opt.start,opt.count]);
							break;								
					}
					break;
				
				case "WHERE":
				case "HAVING":

					me.nowhere = false;
					
					switch (opt.constructor) {
						/*case Array:
							
							switch (opt.length) {
								case 0:
									break;
								
								case 1:
									me.query += ` ${key} ?? IS NULL`;
									me.opts.push( opt[0] );
									break;
									
								case 2:
									me.query += ` ${key} ?? LIKE '${opt[1]}'`;
									me.opts.push( opt[0] );
									break;
									
								case 3:
									me.query += ` ${key} ?? BETWEEN ? AND ?`;
									me.opts.push( opt[0] );
									me.opts.push( opt[1] );
									me.opts.push( opt[2] );
									break;
									
								default:
							}
							
							break;*/
							
						case String:
						
							me.safe = false;
							me.query += ` ${key} ${opt}`;
							break;

						case Object:
							var rels = []; 
							for (var n in opt) {
								if (opt[n] == null) {		// using unsafe expression query (e.g. &x<10)
									me.safe = false;
									rels.push(n);
									delete opt[n];
								}
								else 						// using unsafe range query (e.g. &x=[min,max])
								if ( (args = (opt[n]+"").split(",")).length>1 ) {
									me.safe = false;
									rels.push(sql.escapeId(n) + " BETWEEN " + args[0] + " AND " + args[1] );
									delete opt[n];
								}
								// otherwise using safe query (e.g &x=value)
							}
							
							for (var n in opt) { 			// aggregate where clause using least,sum,etc
								rels.push(ag);
								me.opts.push(opt);
								break;
							}
									
							rels = rels.join(" AND "); 		// aggregate remaining clauses
							if (rels)
								me.query += ` ${key} ${rels}`;
								
							else
								me.nowhere = true;
							
							break;
							
						default:
								me.unsafe = true;
					}
					break;
					
				case "ORDER":
					switch (opt.constructor) {
						case Array:
							var by = [];
							opt.each(function (n,opt) {
								if (opt.property)
									by.push(`${opt.property} ${opt.direction}`);
								else
									for (var n in opt) 
										by.push(`${n} ${opt[n]}`);
							});
							me.query += ` ${key} ${by.join(",")}`;
							break;
							
						case String:
							me.query += ` ${key} ??`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							break;
					}
					break;
					
				case "SET":
					
					switch (opt.constructor) {
						/*case Array:
							me.safe = false;
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;*/
							
						case String:
							me.safe = false;
							me.query += ` ${key} ${opt}`;
							break;

						case Object:
							
							me.query += ` ${key} ?`;
							me.opts.push(opt);
							break;
							
						default:
							me.unsafe = true;
					}
					break;
					
				default:
					switch (opt.constructor) {
						case Array:
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${key} ??`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							me.query += ` ${key} ?`;
							me.opts.push(opt);
							break;
					}
			}

	},
	
	update: function (req,res) {
		
		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where.ID ,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
			
		me.x(table, "UPDATE");
		me.x(req, "SET");
		me.x(me.where, "WHERE "+(me.ag||""));
		me.x(me.order, "ORDER BY");
		
		if (me.nowhere)
			res( SQL.errors.unsafeQuery );

		else
		if (me.safe || me.unsafeok)
			journal( function () {
				
				sql.query(me.query, me.opts, function (err,info) {

					if (res) res( err || info );

					if (SQL.emit && ID && !err) 		// Notify clients of change.  
						SQL.emit( "update", {
							table: me.table, 
							body: req, 
							ID: ID, 
							from: client
							//flag: flags.client
						});

				});

				if (me.trace) Trace(me.query);

			});
		
		else
		if (res)
			res( SQL.errors.unsafeQuery );
			
	},
	
	select: function (req,res) {

		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
			
		me.x(me.index || "*", "SELECT SQL_CALC_FOUND_ROWS");
		me.x(me.nlp, "");
		me.x(me.bin, "IN BINARY MODE");
		me.x(me.qex, "WITH QUERY EXPANSION");
		me.x(table, "FROM");
		me.x(me.join, "JOIN", {});
		me.x(me.where, "WHERE "+(me.ag||""));
		me.x(me.having, "HAVING "+(me.ag||""));
		me.x(me.order, "ORDER BY");
		me.x(me.group, "GROUP BY");
		me.x(me.limit, "LIMIT");

		if (me.safe || me.unsafeok)
			switch (req.name) {
				case "each": 
					sql.query(me.query, me.opts)
					.on("error", function (err) {
						req(err,me);
					})						
					.on("result", function (rec) {
						req(rec,me);
					}); 
					break;
				
				case "clone": 
					var rtn = [];
					sql.query(me.query, me.opts, function (err,recs) {	
						if (err) return req( err, me );
						
						recs.each(function (n,rec) {
							rtn.push( Copy(rec,{}) );
						});
						
						req(rtn,me);
					});
					break;
				
				case "all":
				default:  
					sql.query(me.query, me.opts, function (err,recs) {	
						req( err || recs, me );
					});
			}
		
		else
		if (res)
			res( SQL.errors.unsafeQuery );
		
		if (me.trace) Trace(me.query);
					
	},
	
	delete: function (req,res) {
		
		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
		
		me.x(table, "DELETE FROM");
		me.x(me.where, "WHERE "+(me.ag||""));
		
		if (me.nowhere)
			res( SQL.errors.unsafeQuery );	
		
		else
		if (me.safe || me.unsafeok)
			journal( function () {
			
				me.sql.query(me.query, me.opts, function (err,info) {

					if (me.res) me.res(err || info);
				
					if (SQL.emit && ID && !err) 		// Notify clients of change.  
						SQL.emit( "delete", {
							table: me.table, 
							ID: ID, 
							from: me.client
							//flag: flags.client
						});
					
				});
					
				if (me.trace) Trace(me.query);
				
			});
	
		else
		if (res)
			res( SQL.errors.unsafeQuery );		
	},
	
	insert: function (req,res) {
		
		function isEmpty(obj) {
			for (var n in obj) return false;
			return true;
		}
		
		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true; 
		
		if (!req.length) req = [{}];   // force at least one insert
		
		req.each(function (n,rec) {
			sql.query(
				me.query = isEmpty(rec)
					? " INSERT INTO ?? VALUE ()"
					: " INSERT INTO ?? SET ?" ,

					[table,rec], function (err,info) {

				if (!n && res) { 					// respond only to first insert
					res( err || info );

					if (SQL.emit && !err) 		// Notify clients of change.  
						SQL.emit( "insert", {
							table: me.table, 
							body: rec, 
							ID: info.insertId, 
							from: client
							//flag: flags.client
						});
				}			

			});

			if (me.trace) Trace(me.query);
		});

	},

	get rec() { 
	},
	
	unlock: function (ID, cb, lockcb) {  			// unlock record 
		var me = this,
			sql = me.sql,
			lockID = {Lock:`${me.table}.${ID}`, Client:me.client};
		
		sql.query(
			"DELETE FROM openv.locks WHERE least(?)", 
			lockID, 
			function (err,info) {
				
			if (info.affectedRows) {
				cb();
				sql.query("COMMIT");  // commit queues transaction
			}
			
			else
			if (lockcb)
				sql.query(
					"INSERT INTO openv.locks SET ?",
					lockID, 
					function (err,info) {
						
					if (err)
						me.res( "record already locked by another" );

					else
						sql.query("START TRANSACTION", function (err) {  // queue this transaction
							lockcb();
						});
				});	
			
			else
				me.res( "record must be locked" );
		});
	},

	set rec(req) { 									// crud operation
		var me = this,
			res = me.res;
		
		if (req) 
			switch (req.constructor) {
				case Error:
				
					if (res) res(req);
					break;
					
				case Array: 

					me.insert(req,res);
					break;
					
				case Object:

					me.update(req,res);
					break;
					
				case Function:
				
					me.select(req,res);
					break;
					
				default:
				
					if (me.trace) Trace(
						`${req.toUpperCase()} ${me.table} FOR ${me.client} ON ${CLUSTER.isMaster ? "MASTER" : "CORE"+CLUSTER.worker.id}`
					);
				
					switch (req) {
						case "lock.select":

							me.rec = function (recs) {

								if (recs.constructor == Error) 
									res( recs+"" );
								
								else
								if (rec = recs[0]) 
									me.unlock(rec.ID, function () {
										res( rec );
									}, function () {
										res( rec );
									});
								
								else
									res( "no record" );
								
							};
							
							break;
						
						case "lock.delete":
							
							me.unlock(ID, function () {
								me.rec = null;
							});
							break;
													
						case "lock.insert":

							me.unlock(ID, function () {
								me.rec = [me.data];
							});
							break;
							
						case "lock.update":

							me.unlock(ID, function () {
								me.rec = me.data;
							});
							break;
							
						case "lock.execute":
							
							res( "execute undefined" );
							break;

						case "select": me.rec = me.res; break;
						case "update": me.rec = me.data; break;
						case "delete": me.rec = null; break;
						case "insert": me.rec = [me.data]; break;
						case "execute": me.rec = SQL.errors.unsupportedQuery; break;
						
						default:
							me.rec = SQL.errors.invalidQuery;
					}

			}
		
		else 
			me.delete(req,res);
		
		return this;
	}
	
};

function context(ctx,cb) {
	var sql = this;
	var context = {};
	for (var n in ctx) context[n] = new SQL.DSVAR(sql,ctx[n],{table:"app1."+n});
	if (cb) cb(context);
}

function crude(req,res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags;

	sql.context({ds: {
			trace:	true,
			table:	req.table,
			where:	req.query,
			res:	res,
			order:	flags.sort,
			browse: flags.pivot,
			group: 	flags.pivot || flags.tree,
			score:	flags.score,
			limit: 	flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
			index: 	flags.index,
			data:	req.body,
			client: req.client,
			unsafe:	false	
		}}, function (ctx) {
			
		ctx.ds.rec = (flags.lock ? "lock." : "") + req.action;
		
	});
}


// SQL engines interface

SQL.select.sql = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		sql.query(
			"SELECT SQL_CALC_FOUND_ROWS * FROM ?? "+(rec.Code||"WHERE least(?,1)").format(query),
			["app1."+log.Table,query],
			function (err, recs) {

				res(err || recs);
		});
	});
}

SQL.insert.sql = function Insert(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"INSERT INTO ?? "+(rec.Code||"SET ?"),
				["app1."+rec.Name, req.body],
				function (err, recs) {
					
			res(err || recs);
		});
	});
}

SQL.delete.sql = function Delete(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"DELETE FROM ?? "+(rec.Code||"WHERE least(?,1)").format(query),
				["app1."+log.Table,query],
				function (err, recs) {	
					
			res(err || recs);
		});
	});
}

SQL.update.sql = function Update(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"UPDATE ?? "+(rec.Code||"SET ? WHERE least(?,1)").format(query),
				["app1."+rec.Name, req.body,query],
				function (err, recs) {	
					
			res(err || recs);
		});
	});
}

// GIT interface

SQL.select.baseline = function Baseline(req,res) {

	var ex = {
		group: `mysqldump -u${ENV.MYSQL_USER} -p${ENV.MYSQL_PASS} ${req.group} >admins/db/${req.group}.sql`,
		openv: `mysqldump -u${ENV.MYSQL_USER} -p${ENV.MYSQL_PASS} openv >admins/db/openv.sql`,
		commit: `git commit -am "${req.client} baseline"`
	};
	
	res(SUBMITTED);
	
	CP.exec(ex.group, function (err,log) {
		
		Trace("CHECKPT "+(err||"OK"));
		
		CP.exec(ex.openv, function (err,log) {
			
			Trace("CHECKPT "+(err||"OK"));
			
			CP.exec(ex.commit, function (err,log) {
				Trace("COMMIT "+(err||"OK"));
			});
		});
	});

}

SQL.select.git = function Select(req, res) {

	var gitlogs = 'git log --reverse --pretty=format:"%h||%an||%ce||%ad||%s" > gitlog';
	
	CP.exec(gitlogs, function (err,log) {
			
		if (err)
			res(err);
		else
			FS.readFile("gitlog", "utf-8", function (err,logs) {
				var recs = [], id=0;
				
				logs.split("\n").each( function (n,log) {
					
					var	parts = log.split("||");
					
					recs.push({	
						ID: id++,
						hash: parts[0], 
						author: parts[1],
						email: parts[2],
						made: new Date(parts[3]),
						cm: parts[4]
					});
					
				});
				
				res(recs);
				
			});
	});
						
}

// EMAIL peer-to-peer exchange interface
	
SQL.select.unsent = function Select(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	sql.query("SELECT * FROM emails WHERE Pending", function (err,recs) {
		res(err || recs);

		sql.query("UPDATE emails SET Pending=0 WHERE Pending");
	});
	
}
	
SQL.execute.unsent = function Execute(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	res(SUBMITTED);
	
	if (false)
	requestService(srv, function (rec) {
		sendMail({
			from:  rec.From, //"g5120089@trbvm.com",
			to:  rec.To,
			subject: rec.Subject,
			html: rec.Body,
			alternatives: [{
				contentType: 'text/html; charset="ISO-59-1"',
				contents: ""
			}]
		}, function (err,info) {
			if (err) console.log(err);
		});
	});
	
}

// Master catalog interface

SQL.select.CATALOG = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;

	if (flags.has)
		sql.query(
			"SELECT *,count(ID) as Count FROM catalog WHERE INSTR(Content,?)", 
			flags.has, function (err,recs) {

			res(err || recs);
			
			if ( !recs.Count )
				sql.hawkCatalog(req,res);
			
		});
	else
		sql.query(
			"SELECT * FROM catalog", 
			function (err,recs) {

			res(err || recs);

		});
	
}

SQL.execute.CATALOG = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;

	var catalog = {
		files: "Tags,Tagger,Area",
		intake: "Special,Name,App,Tech",
		engines: "Code,engine,Classif,Special",
		news: "Message",
		sockets: "Location,Client,Message",
		detects: "label"
	};

	var limits = {
		score: 0.1,
		records: 100,
		stamp: new Date(),
		pivots: flags.pivot || ""
	};
		
	res(SUBMITTED);
	
	sql.query("DELETE from catalog");
	
	sql.flattenCatalog( flags, catalog, limits, function (recs) {
		
		recs.each( function (n,rec) {
			sql.query("INSERT INTO catalog SET ?",rec);
		});
		
	});
	
}

// Getters

SQL.select.ACTIVITY = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var recs = {};
	
	sql.query(
		"SELECT * FROM roles", 
		[] , 
		function (err,roles) {
			
	sql.query(
		'SELECT `Table`,sum(action="Insert") AS INSERTS,sum(action="Updates") as UPDATES, sum(action="Select") as SELECTS,sum(action="Delete") as DELETES,sum(action="Execute") as EXECUTES  FROM dblogs WHERE datediff(now(),Event)<30 GROUP BY `Table`',
		[],
		function (err,acts) {
			
			roles.each( function (n,role) {
				var rec = recs[role.Table];
				
				if (!rec) rec = {
					INSERT: 0,
					SELECTS: 0,
					DELETES: 0,
					UPDATES: 0,
					EXECUTES: 0
				};
				
				Copy( role, rec );
			});
			
			acts.each( function (n,act) {
				var rec = recs[act.Table];
				
				if (!rec) rec = recs[act.Table] = {
					Table: act.Table,
					INSERT: "guest",
					DELETE: "guest",
					UPDATE: "guest",
					SELECT: "guest",
					EXECUTE: "guest"
				};
				
				Copy( act, rec );
			});
			
			res( SQL.listify(recs, "ID") );
	});
	});
}

SQL.select.VIEWS = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var views = [];
	var path = `./public/jade/`;
	
	SQL.indexer( path , function (files) {
		
		files.each(function (n,file) {
		
			var stats = FS.statSync(path + file);		
			
			if (stats.isDirectory())
				views.push( {ID: n, Name: file, class: file+SLASH} );
				
		});
	});
	
	res(views);
}

SQL.select.LINKS = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var type = unescape(query.class || query.area || "");
	var path = `./public/jade/${type}/`;
	var id = 0;
	var taken = {};
	var links = [{
		ID: id++,
		Name: "&diams; HOME".tag("a",{
			href: "home.view"
		})
	}];

	SQL.indexer( path, function (files) {

		files.each(function (n,file) {
		
			var stats = FS.statSync(path + file);
			
			if (stats.isDirectory()) 
				if (file.charAt(0) == ".") 
					SQL.indexer( path+file, function (names) {
						
						names.each(function (n,name) {
							links.push({
								ID: id++,
								Name: (file.substr(1)+" &rrarr; "+name).tag("a",{
									href: `${name}.view`
										/*(SQL.STATEFUL[name] ? SQL.WORKER : "") + area + name +".view"*/
								})
							});
							taken[name] = 1;
						});
					});
				else 
					links.push({
						ID: id++,
						Name: ("&diams; "+file).tag("a",{
							href: file + ".home.view"
						})
					});
			else {
				var name = file.replace(".jade","");
				
				if (!taken[name])
					links.push({
						ID: id++,
						Name: name.tag("a",{
							href: `${name}.view`
							/*(SQL.STATEFUL[name] ? SQL.WORKER : "") + area + file*/
						})
					});
			}
			
		});
	});
	
	res(links);
}

/*
SQL.select.THEMES = function (req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var themes = [];
	var path = ENV.THEMES;

	SQL.indexer( path , function (n,file) {
		var stats = FS.statSync(path + file);		
		
		if (!stats.isDirectory())
			themes.push( {ID: n, Name: file, theme: file} );
	});
	
	res(themes);	
}
*/

SQL.select.SUMMARY = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var cnts = SQL.PULSE.COUNTS;
	
	// Could check rtn.serverStatus and warningCount (become defined when a MySQL table corrupted).  
	// Return selected record info (fieldCount, affectedRows, insertId, serverStatus, warningCount, msg)
				
	res(cnts.State
		? [ {ID:0, Name: cnts.State},
			{ID:1, Name: JSON.stringify(cnts).replace(/"/g,"").tag("font",{color: (cnts.State=="ok") ? "green" : "red"})}
		  ]
		: [ {ID:0, Name: "pending"} ]
	);
}

SQL.select.USERS = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,userinfo(Client,Org,Location) AS Name FROM sockets WHERE least(?,1) ORDER BY Client", 
		guardQuery(query,true),
		function (err, recs) {
			res(err || recs);
	});
}

SQL.select.ENGINES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,engineinfo(Name,Engine,Updated,Classif,length(Code),Period,Enabled,length(Special)) AS Name FROM engines WHERE least(?,1) ORDER BY Name",
		guardQuery(query,true), 
		function (err, recs) {
			res(err || recs);
	});
}

SQL.select.CONFIG = function Select(req, res) {
	var sql = req.sql;
	
	CP.exec("df -h", function (err,dfout,dferr) {
	CP.exec("netstat -tn", function (err,nsout,nserr) {
	CP.exec("npm list", function (err,swconfig) {
		res({	
			sw: escape(swconfig)
					.replace(/\%u2502/g,"")
					.replace(/\%u2500/g,"")
					.replace(/\%u251C/g,"")
					.replace(/\%u252C/g,"")
					.replace(/\%u2514/g,"")
					.replace(/\%0A/g,"")
					.replace(/\%20/g,""),
			classif: SQL.CLASSIF,
			services: nsout,
			disk: dfout,
			cpu: 
				(OS.uptime()/3600/24).toFixed(2)+" days " 
					+ OS.loadavg() + "% used at [1,2,3] min " 
					+ OS.cpus()[0].model, // + " at " + ENGINE.temp() + "oC",
			
			platform: OS.type()+"/"+OS.platform()+"/"+OS.arch(),
			
			memory: 
				((OS.totalmem()-OS.freemem())*1e-9).toFixed(2) + " GB " 
				+ (OS.freemem() / OS.totalmem()).toFixed(2) + " % used",
			
			host: OS.hostname() 			// do not provide in secure mode
			//cpus: JSON.stringify(OS.cpus()),
			//routes: JSON.stringify(SQL.routes),
			//netif: JSON.stringify(OS.networkInterfaces()), 	//	do not provide in secure mode
			//temp: TEMPIF.value()  // unavail on vms
		});
	});
	});
	});
}
	
SQL.select.TABLES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var rtns = [], ID=0;
	
	sql.query("SHOW TABLES")
	.on("result", function (rec) {
		rtns.push({
			Name: rec.Tables_in_app1.tag("a",{href:"/"+rec.Tables_in_app1+".db"}),
			ID: ID++
		});
	})
	.on("end", function () {
		for (var n in SQL.select)
			rtns.push({
				Name: n.tag("a",{href:"/"+n+".db"}),
				ID: ID++
			});
			
		res(rtns);
	});
}

SQL.select.ADMIN = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT *,avg_row_length*table_rows AS Used FROM information_schema.tables", [], function (err, recs) {
		res(err || recs);
	});
}
	
SQL.select.QUEUES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,queueinfo(Job,min(Arrived),timediff(now(),min(Arrived))/3600,Client,Class,max(State)) AS Name FROM queues GROUP BY Client,Class", 
		[],
		function (err, recs) {
			res(err || recs);
	});
}

SQL.select.CLIQUES = function v(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	 
	res([]);
	/*
	sql.query(
		  "SELECT dblogs.ID AS ID,cliqueinfo(dblogs.ID,count(DISTINCT user),concat(ifnull(role,'none'),'-',ifnull(detectors.name,'nill')),group_concat(DISTINCT user,';')) AS Name "
		  + "FROM dblogs LEFT JOIN detectors ON (detectors.id=dblogs.recid AND dblogs.table='detectors') WHERE recid AND least(?,1) GROUP BY role,detectors.name",
		
		guardQuery(query,true),  function (err, recs) {
			res(err || recs);
	});*/
}

SQL.select.HEALTH = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var isp = "ISP".tag("a",{href:req.isp});
	var asp = "ASP".tag("a",{href:req.asp});
	var now  = new Date();
	var rtns = [], ID = 0;
	
	var dbstats = {
		Innodb_data_written: 1,
		Innodb_data_read: 1,
		Queries: 1, 
		Uptime: 1
	};

	CP.exec("df -h /home", function (err,dfout,dferr) {
//console.log(dfout);

	CP.exec("netstat -tn", function (err,nsout,nserr) {
//console.log(nsout);

	sql.query(
		"SELECT "
		+ "round(avg(LinkSpeed)*1e-3,2) AS avg_KBPS, "
		+ "round(max(LinkSpeed)*1e-3,2) AS max_KBPS, "
		+ "sum(Fault !='') AS faults, "
		+ "round(sum(Overhead)*1e-9,2) AS tot_GB, "
		+ "count(DISTINCT Client) AS clients, "
		+ "count(ID) AS logs "
		+ "FROM dblogs")
	.on("error", function (err) {
		console.log(err);
	})
	.on("result", function (lstats) {

//console.log(lstats);

	sql.query(
		  "SELECT "
		+ "sum(departed IS null) AS backlog, "
		+ "avg(datediff(ifnull(departed,now()),arrived)) AS avg_wait_DAYS, "
		+ "sum(RunTime)*? AS cost_$ "
		+ "FROM queues",[4700])
	.on("error", function (err) {
		console.log(err);
	})
	.on("result", function (qstats) {
	
//console.log(qstats);

	sql.query("SHOW GLOBAL STATUS", function (err,dstats) {
		
//console.log(dstats);

	sql.query("SHOW VARIABLES LIKE 'version'")
	.on("result", function (vstats) {
	
		dstats.each( function (n,stat) { 
			if ( stat.Variable_name in dbstats )
				dbstats[stat.Variable_name] = stat.Value; 
		});
		
//console.log(dbstats);

		Each(qstats, function (n,stat) {
			rtns.push({ID:ID++, Name:"job "+n.tag("a",{href:"/admin.jade?goto=Jobs"})+" "+stat});
		});

		var stats = {
			disk_use: dfout.split("\n")[1],
			cpu_up_DAYS: (OS.uptime()/3600/24).toFixed(2),
			cpu_use: (OS.loadavg()[2]*100).toFixed(2) + "% @ ", //  + ENGINE.temp() + "oC",
			memory_use: 
				((OS.totalmem()-OS.freemem())*1e-9).toFixed(2) +" GB " 
				+ (OS.freemem() / OS.totalmem()).toFixed(2) + " %"
		};
			
		Each(stats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.jade?goto=Logs"})+" "+stat});
		});

		Each(lstats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.jade?goto=Logs"})+" "+stat});
		});

		var stats = {
			rw_GB: 
				(dbstats.Innodb_data_read*1e-9).toFixed(2) + "/" +
				(dbstats.Innodb_data_written*1e-9).toFixed(2),
				
			tx_K: (dbstats.Queries*1e-3).toFixed(2),
	
			up_DAYS: (dbstats.Uptime/3600/24).toFixed(2)			
		};
			
		Each(stats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.jade?goto=DB Config"})+" "+stat});
		});

		var nouts = nsout ? nsout.split("\n") : ["",""],
			naddr = nouts[1].indexOf("Local Address");

		var stats = {
			classif: SQL.CLASSIF,
			cpu: OS.cpus()[0].model,
			host: OS.hostname() 			// do not provide in secure mode
			//cpus: JSON.stringify(OS.cpus()),
			//routes: JSON.stringify(SQL.routes),
			//netif: JSON.stringify(OS.networkInterfaces()), 	//	do not provide in secure mode
			//temp: ENGINE.temp()  // unavail on vms
		};
	
		nouts.each(function (n,nstat) {
			if (n > 1)
				stats[nstat.substr(naddr,13)] = "connected";
		});
		
		stats[OS.platform()] = OS.release();
		stats.mysql = vstats.Value;

		Each(stats, function (n,stat) {
			rtns.push({ID:ID++, Name:asp+" "+n.tag("a",{href:"/admin.jade?goto=SWAPs"})+" "+stat});
		});

		res(rtns);
	});
	/*
	.on("end", function () {
		
		var maxage = age = vol = files = 0;
		
		for (var area in {UPLOADS:1, STORES:1, SHARES:1} )
			SQL.indexer( ENV[area], function (n,file) {
				var stats = FS.statSync(ENV[area] + file);
		
				age += (now.getTime()-stats.atime.getTime())*1e-3/3600/24;
				vol += stats.size*1e-9;
				if (age > maxage) maxage = age;
				files++;
			});
			
		var fstats = {
			avgAgeDays: (age/files).toFixed(2),
			maxAgeDays: maxage.toFixed(2),
			volumeGB: vol.toFixed(2),
			files: files
		};
		
		Each( fstats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" Filesystem "+n+" "+stat});
		});

		res(rtns);
	}); 
	* */
	}); 
	});
	});
	});
	});

}

SQL.select.likeus = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	sendMail({
		from: SQL.SITE.POC,
		to:  SQL.EMAIL.HAWKS,
		subject: req.client + " likes " + SQL.SITE.title + " !!",
		html: "Just FYI",
		alternatives: [{
			contentType: 'text/html; charset="ISO-59-1"',
			contents: ""
		}]
	}, function (err,info) {
		if (err) console.log(`EMAIL ${err}`);
	});
	
	sql.query("INSERT INTO openv.profiles SET ? ON DUPLICATE KEY UPDATE Likeus=Likeus+1,QoS=least(Likeus+1,5)",{ 
		LikeUs:0, 
		QoS:0,
		Banned:"",
		Joined: new Date(),
		Updated: new Date(),
		Client:req.client 
	})
	.on("error", function (err) {
		console.log("Failed to update profile "+err);
	});

	res( `Thanks ${req.client} for liking ${SQL.SITE.nick} !  Check out your new ` +
		"QoS profile".tag("a",{href:'/profile.jade'}) + " !" );

}

SQL.select.tips = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	var q = sql.query(
		"SELECT *, "
		+ "count(detects.ID) AS weight, "
		+ "concat('/tips/',chips.ID,'.jpg') AS tip, "
		+ "concat("
		+ 		"linkquery('O', 'https://ldomar.ilabs.ic.gov/omar/mapView/index',concat('layers=',collects.layer)), "
		+ 		"linkquery('S', '/minielt.jade',concat('src=/chips/',chips.name))"
		+ ") AS links, "
		+ "datediff(now(),collected) AS age "
		+ "FROM detects "
		+ "LEFT JOIN chips ON MBRcontains(chips.address,detects.address) "
		+ "LEFT JOIN collects ON collects.ID = chips.collectID "
		+ "LEFT JOIN detectors ON detectors.name = detects.label "
		+ "WHERE least(?,1) "
		+ "GROUP BY detects.address "
		+ "ORDER BY detects.made DESC "
		+ "LIMIT 0,400", 
		guardQuery(query,true), 
		function (err, recs) {

			if (err)
				console.log([err,q.sql]);
			else
				recs.each(function(n,rec) {
					rec.ID = n;
					rec.lat = rec.address[0][0].x*180/Math.PI;
					rec.lon = rec.address[0][0].y*180/Math.PI;
					//delete rec.address;
				});
			
			res(err || recs);
	});
}

// Execute engines

SQL.execute.intake = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	sql.query("SELECT intake.ID,count(dblogs.ID) AS Activity,max(dblogs.Event) AS LastTx FROM intake LEFT JOIN dblogs ON dblogs.RecID=intake.ID  group by intake.ID HAVING LEAST(?,1)",query)
	.on("result", function (log) {
//console.log(log);
	
	sql.query("SELECT Name,Special FROM intake WHERE ? AND Special IS NOT NULL AND Special NOT LIKE '//%'",[{ID:log.ID},{Special:null}])
	.on("result",function (sys) {
//console.log(sys);

	var conpdf = {};
	var maxpdf = 0;
	var cnts = 0;
	
	sql.query("SELECT Value,max(Event),Client,count(ID) AS Counts FROM dblogs WHERE `Table`='intake' AND Field='TRL' GROUP BY Client HAVING ?",{RecID: sys.ID})
	.on("result", function (client) {
		cnts += client.Counts;

		if (conpdf[client.Value])
			conpdf[client.Value]++;
		else
			conpdf[client.Value] = 1;
			
		if (conpdf[client.Value] > maxpdf) maxpdf = conpdf[client.Value];
	})
	.on("end", function () {
//console.log([cnts,maxpdf]);

	sql.query("SELECT count(ID) AS Count FROM queues WHERE LEAST(?)",{State:0,Job:sys.Name,Class:"tips"})
	.on("result", function (users) {

	if (false)	// update if unset
		sql.query("SELECT "+stat.joinify()+" FROM intake WHERE ?",{ID:sys.ID})
		.on("result",function (sysstat) {
			
		for (var n in stat) if (!sysstat[n]) sysstat[n] = stat[n];
			
		sql.query("UPDATE intake SET ? WHERE ?",[sysstat,{ID: sys.ID}], function () {
//console.log("updated");	
		}); // update
		}); // stats
	else 		// unconditionally update
		sql.query("UPDATE intake SET ? WHERE ?",[stat,{ID: sys.ID}], function () {
//console.log("updated");	
		}); // update
	
	}); // users
	}); // clients
	}); // sys
	}); // log
	
	res(SUBMITTED);
}					

/*
SQL.update.engines = function Update(req, res) {
	var sql = req.sql, log = req.log, query = req.query, body = req.body;

	sql.query("UPDATE engines SET ? WHERE ?", [body,query])
	.on("end", function () {
		ENGINE.free(req,res);
	});
}*/

SQL.execute.engines = function Execute(req, res) {
	var sql = req.sql, query = req.query, body = req.body;
	var Engine = query.Engine, Name = query.Name;
	
	if (Engine && Name)
		sql.query(
			"SELECT *,count(ID) AS Found FROM engines WHERE LEAST(?) LIMIT 0,1",
			{Engine:Engine,Name:Name}
		)
		.on("error", function (err) {
			res(err);
		})
		.on("result", function (eng) {

			if (eng.Found)  				// existing engine
				switch (eng.Engine) {
					case "select":			// update SQL crude interface
					case "delete":
					case "update":
					case "insert":
					case "execute":
						
						if (eng.Enabled)
							res( SQL.errors.disableEngine );
						
						else 
							compileEngine(Engine,Name,eng.Code,res);
						
						break;
						
					case "jade":			// update jade skin
						
						if (eng.Enabled)
							res( SQL.errors.disableEngine );

						else
							try {
								FS.readFile(
									`./public/${eng.Engine}/${eng.Name}.${eng.Engine}`, 
									'utf-8', function (err,buf) {
									
									if (err) 
										res(err);
									
									else
										sql.query("UPDATE engines SET ? WHERE ?", [
											{Code: buf, Updated: new Date()}, 
											{ID:eng.ID}
										], function (err) {
											res(err || "ok");
										});
								});
							}
							catch (err) {
								res( SQL.errors.noEngine );
							}
								
						break;
						
					default:				// run simulation engine against located engine
						req.query = (eng.Vars.query||"").parse({});
						req.table = eng.Name;
						ENGINE.read(req, res);
				}
			else 							// create new engine
				switch (Engine) {
					case "select":		// create crude interface
					case "delete":
					case "update":
					case "insert":
					case "execute":

						sql.query("INSERT into engines SET ?", {
							Code: (SQL[Engine][Name]+""),
							Enabled: 0,
							Name: Name,
							Updated: new Date(),
							Engine: Engine
						}, function (err) {
							res(err || "ok");
						});
							
						break;

					default:
					
						FS.readFile(
							`.public/${Engine}/${Name}.${Engine}`,
							'utf-8', function (err,buf) {
							
							if (err) 
								res(err);
								
							else
								sql.query("INSERT into engines SET ?", {
									Code: buf,
									Enabled: 0,
									Name: Name,
									Engine: Engine
								}, function (err) {
									res(err || "ok");
								});
						});
				}
			
		});
	else
		res( SQL.errors.missingEngine );
}

SQL.execute.news = function Execute(req, res) {  
	var sql = req.sql, log = req.log, query = req.query;
	var mask = {
		today: new Date(),
		me: req.client
	};

	// export - email/socket those on to list anything marked new

	res(SUBMITTED);
	
	sql.query("SELECT * FROM news WHERE New AND least(?,1)", query)
	.on("result", function (news) {
		//sql.query("UPDATE news SET ? WHERE ?",[{New:false},{ID:news.ID}]);

		news.Message.split("\n").each( function (n,line) {

			var parts = line.split("@"),
				subj = parts[0],
				to = parts[1].substr(0,parts[1].indexOf(" ")),
				body = parts[1].substr(to.length);

			console.log("NEWS -> "+to);

			switch (to) {
				case "conseq":

				case "wicwar":

				case "jira":
					break;

				default:

					sendMail({
						from: SITE.POC,
						to:  to,
						subject: subj,
						html: body.format(mask),
						alternatives: [{
							contentType: 'text/html; charset="ISO-8859-1"',
							contents: ""
						}]
					});
			}
		});
	});
		
	// import
	
	sql.query("SELECT ID,datediff(now(),Starts) AS Age, Stay FROM news HAVING Age>Stay")
	.on("result", function (news) {
		sql.query("DELETE FROM news WHERE ?",{ID:news.ID});
	});
	
	sql.query("UPDATE news SET age=datediff(now(),Starts)");
	
	sql.query("UPDATE news SET fuse=Stay-datediff(now(),Starts)");
	
	sql.query("SELECT * FROM news WHERE Category LIKE '%/%'")
	.on("result", function (news) {  
		var parts = news.Category.split("/"), name = parts[0], make = parts[1], client = "system";

		sql.query(
			  "SELECT intake.*, link(intake.Name,concat(?,intake.Name)) AS Link, "
			+ "link('dashboard',concat('/',lower(intake.Name),'.jade')) AS Dashboard, "
			+ "sum(datediff(now(),queues.Arrived)) AS Age, min(queues.Arrived) AS Arrived, "
			+ "link(concat(queues.sign0,queues.sign1,queues.sign2,queues.sign3,queues.sign4,queues.sign5,queues.sign6,queues.sign7),concat(?,intake.Name)) AS Waiting, "
			+ "link(states.Name,'/parms.jade') AS State "
			+ "FROM intake "
			+ "LEFT JOIN queues ON (queues.Client=? and queues.State=intake.TRL and queues.Class='TRL' and queues.Job=intake.Name) "
			+ "LEFT JOIN states ON (states.Class='TRL' and states.State=intake.TRL) "
			+ "WHERE intake.?", ["/intake.jade?name=","/queue.jade?name=",client,{ Name:name }] ) 
		.on("error", function (err) {
			console.log("news apperr="+err);
		})
		.on("result", function (sys) {
			var msg = sys.Link+" "+make.format(sys);
			
			sql.query("UPDATE news SET ? WHERE ?", [
				{	Message: msg,
					New: msg != news.Message ? -1 : 0
				}, 
				{ID:news.ID}
			]);
		});
	});
}

SQL.execute.milestones = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var map = {SeqNum:1,Num:1,Hours:1,Complete:1,Task:1};

	for (var n=0;n<=10;n++) map["W"+n] = 1;

	sql.query("DELETE FROM openv.milestones");
	SQL.RDR.xlsx(sql,"milestones.xlsx","stores",function (rec) {
		for (var n in map) map[n] = rec[n] || "";
		
		sql.query("INSERT INTO openv.milestones SET ?",map, function (err) {
			if (err) console.log("apperr="+err);
		});
	});

	res(SUBMITTED);
}

SQL.execute.sockets = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	if (SQL.emit)
		sql.query("SELECT * FROM sockets WHERE length(Message)")
		.on("result", function (sock) {
			SQL.emit("alert", {msg: sock.Message, to: sock.Client, from: req.client});
			sql.query("UPDATE sockets SET ? WHERE ?",[{Message:""},{ID:sock.ID}]);
		});
		
	res(SUBMITTED);
}

// Files interface

SQL.select.uploads = SQL.select.stores = function Uploads(req, res) {
	var sql = req.sql, log = req.log, query = req.query,  body = req.body;
	var now = new Date();			
	var rtns = [], area = req.table;
	var path = `./public/${area}`;

	switch (area) {
		
		case "uploads":
		case "stores":
		case "proofs":
		case "shares":
		
			SQL.indexer( path, function (files) {
				
				files.each( function (n,file) {
					var link = `/${area}/${file}`;

					switch (area) {
						case "proofs":
						case "shares":
							
							var parts = file.split(".");

							switch (parts[1]) {
								case "jpg":
								case "png":
								case "ico":
									rtns.push({
										Name	: file.tag("a",{href:"/files.jade?goto=ELT&option="+area+DOT+file}) + "".tag("img",{src:link, width: 32, height: 32}),
										File	: file,
										ID		: n
									});
									break;
									
								default:
									rtns.push({
										Name	: file.tag("a",{href:link}),
										File	: file,
										ID		: n
									});
							}
							break;

						default:
							rtns.push({
								Name	: file.tag("a",{href:link}),
								File	: file,
								ID		: n
							});
							break;
					}
				});
				
			});
			
			res(rtns);

			break;
			
		case "dir": 
		
			for (var area in {uploads:1, stores:1, shares:1} )
				SQL.indexer( `./public/${area}`, function (n,file) {

					var link = `/${area}/${file}`,
						stats = FS.statSync(`./public${link}`);

					rtns.push({
						Link	: link,
						Area	: area,
						Name 	: file,
						Dev 	: stats.dev,
						Mode 	: stats.mode,
						Size	: stats.size,		// Bytes
						Atime 	: stats.atime,
						Mtime	: stats.mtime,
						Isdir	: stats.isDirectory(), 
						Age	: (now.getTime()-stats.atime.getTime())*1e-3/3600/24,	// days
						ID	: n
					});
				});
				
			res(rtns);
			
		default: 
		
			sql.query(
				"SELECT Area,Name,concat(Area,'.',Name) AS Ref, link(Name,concat('/',Area,'.',Name)) AS Link, astext(address) AS Address FROM files HAVING ?", 
				guardQuery(query,true), function (err,rtns) {
					res(err || rtns);
			});
	}
}

SQL.update.uploads = SQL.insert.uploads = function Uploads(req, res) {
	
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
	
	var area = req.table,
		client = req.client,
		name = body.name || "undefined",
		tag = body.tag || "",
		classif = body.classif || "TBD",
		image = body.image,
		geo = body.geo || "POINT(0 0)",
		canvas = body.canvas || {objects:[]},
		attach = [],
		now = new Date(),
		files = [];

	if (image)
		files.push({
			name: name, // + ( files.length ? "_"+files.length : ""), 
			size: image.length/8, 
			image: image
		});
	else
		Each(req.files, function (n,file) {
			files.push({ 
				name: file.name,
				size: file.size,
				path: file.path
			});
		});
				
	res(SUBMITTED);
	
	canvas.objects.each(function (n,obj) {
		
		switch (obj.type) {
			case "image": // ignore blob
				break;
				
			case "rect":
			
				attach.push(obj);

				sql.query("REPLACE INTO proofs SET ?", {
					top: obj.top,
					left: obj.left,
					width: obj.width,
					height: obj.height,
					label: tag,
					made: now,
					name: area+"."+name
				});
				break;			
		}
	});

	SQL.uploader(sql, files, area, function (file) {

		file.Tagger = client;
		file.Tag = tag || "";
		file.Enabled = 1;
		file.Classif = classif;
		file.Revs = 0;
		file.Attach = JSON.stringify(canvas);

		canvas.objects = attach;

		sql.query(	// this might be generating an extra geo=null record for some reason.  works thereafter.
			"INSERT INTO files SET ?,address=geomfromtext(?) ON DUPLICATE KEY UPDATE ?,Revs=Revs+1,address=geomfromtext(?)", 
			[file,geo,{ Tagger: file.Tagger, Added:file.Added }, geo]);

		if (file.image)
			switch (area) {
				case "proofs": 
				
					sql.query("REPLACE INTO proofs SET ?", {
						top: 0,
						left: 0,
						width: file.Width,
						height: file.Height,
						label: tag,
						made: now,
						name: area+"."+name
					});

					sql.query(
						"SELECT detectors.ID, count(ID) AS counts FROM detectors LEFT JOIN proofs ON proofs.label LIKE detectors.PosCases AND proofs.name=? HAVING counts",
						[area+"."+name]
					)
					.on("result", function (det) {
						sql.query("UPDATE detectors SET Dirty=Dirty+1");
					});

					break;
					
			}
	});

}

SQL.execute.uploads = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	
	sql.query("DELETE FROM files WHERE Area LIKE 'geoNode.%'");
	
	CP.exec('git ls-tree -r master --name-only > gitlog', function (err,log) {
			
		if (err)
			res(err);
		else
			FS.readFile("gitlog", "utf-8", function (err,logs) {
				logs.split("\n").each( function (n,log) {
					var parts = ("/"+log).split("/"),
						file = parts[parts.length-1],
						names = file.split("."),
						name = names[0] || "",
						type = "geoNode." + (names[1]||""),
						now = new Date();
						
					if (name)
						sql.query("INSERT INTO files SET ?", {
							Tag: "system",
							Area: type,
							Name: file,
							Added: now,
							L0: parts[0],
							L1: parts[1],
							L2: parts[2],
							L3: parts[3],
							L4: parts[4],
							L5: parts[5]
						});
				});
			});
			
	});
}

SQL.execute.tests = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	res(SUBMITTED);
	
	sql.query(
		"SELECT *,tests.ID AS testID,tests.Channel as chanName,detectors.Name as detName FROM tests "
		+ " LEFT JOIN detectors ON detectors.Channel=tests.Channel AND Enabled HAVING ?",
		{testID:query.ID}
	)
	.on("result", function (test) {
		
console.log(req.profile);
		
		if (SQL.CHIP)
		SQL.CHIP.workflow(sql, {
			detName: test.detName,
			chanName: test.chanName,
			size: test.Feature,
			pixels: test.Pixels,
			scale: test.Pack,
			step: test.SizeStep,
			range: test.SizeRange,
			detects: test.Hits,
			infile: test.FilePath,
			outfile: "/rroc/data/giat/swag/jobs",
			job: {
				client: req.client,
				class: "detect",
				name: test.detName,
				link: test.detName.tag("a",{href:"/swag.jade?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
	});

}

SQL.execute.collects = function Execute(req, res) {
	var req = req, sql = req.sql, log = req.log, query = req.query;
	
	/*
	 * sql each collects w missing collect info {
	 * 		attempt read of file id {
	 * 			error: 
	 * 				sql query elastic (or NCL) search {
	 * 					error or no returned info
	 * 						sched stand order with elastic search (or NCL)
	 * 					ok: 
	 * 						if no rpcs
	 *	 						compute rpc and save with info
	 * 						populate collect with info
	 * 				}
	 * 			ok: read file, gdal parse, 
	 * 				if no rpcs
	 *	 				compute rpc and save with info
	 * 				populate collect with info
	 * 		}
	 * }
	 * 
	 * hydra-like route for NCL alert
	 * 
	 * 	run execeute.collects with this alert
	/*
	sql.each("SELECT * FROM tests", [], function (test) {
		if (test.PollURI)
			requestService({ 		// poll standing order
				url: test.PollURI,
				method: "POST",
				path: "" // soap poll
			}, function (so) {
				if (so.ready) 
					requestService({			// fetch the file
						port: 5200, //8080,
						hostname: "localhost", //'172.31.76.130',
						method: 'GET',
						path: test.FetchURI
					}, function (rec) {

						// need to change logic to accept mime file?

						CP.exec(`gdal --json ${parms.file}`, function (err,gdal) {
							try {
								var meta = JSON.parse(gdal);

								sql.query("INSERT INTO collects SET ?", {
									rows:
									cols:
									bands:
									file:
									layer:
									country:
									collected:
									mission:
									RPCs: "",
									GPCs: ""
								});

							}
							catch (err) {
								console.log("apperr="+err);
							}

						});
						
					});
			});
		else
			requestService({ 		// create standing order
				url: "ncl",
				method: "POT",
				path: "ncl create standing order"  // soap format
			}, function (so) {
				
				sql.query("UPDATE tests SET ? WHERE ?", [{
					PollURI: so.PollURI,
					FetchURI: so.FetchURI
				}, {
					ID:test.ID
				}]);
			});
	});
	
	sql.each("SELECT GPCs FROM collects WHERE GPCs != ''", [], function (collect) {
		sql.query("UPDATE collects SET ?", {
			RPCs: JSON.stringify( computeRPCs( JSON.parse(collect.GPCs) ) )
		});
	});	
	
	sql.each("SELECT * FROM collects WHERE not BVI", [], function (collect) {
	});
	* */
}

SQL.execute.detectors = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	function evalROC(rocs, res) {
		
		rocs.each(function (n,roc) { 		// compute cummulatives for each detector-channel category

			if ( !( roc.category in Qual ) ) {
				qual = totNo = totYes = Qual[roc.category] = 0; 
				f0 = 1; 
			}

			roc.cumNo = totNo += roc.Nno;
			roc.cumYes = totYes += roc.Nyes;

		});
		
		rocs.each(function (n,roc) {		// compute FPR-TPR for each category
			
			f = roc.f;
			
			// Note: qual will not be exact due to (normal) poor f-sampling

			switch (0) {
				case 1: FPR = roc.FPR = f; TPR = 1; break;    	// debug perfect hypo
				case 2: FPR = roc.TPR = f; TPR = 0; break; 		// debug perfect lier
				case 0: 	
					FPR = roc.FPR = roc.cumNo / totNo;
					TPR = roc.TPR = roc.cumYes / totYes;
					break;
			}
			
			df = f0 - f; f0 = f;
			
			if (TPR < FPR) df = -df; 	// assumes hypo/lier knownn a priori - inspect ROC growth
			
			roc.qual = qual += ( (TPR - (1-f)) - (FPR - (1-f)) )*df;
			
//console.log([f,FPR,TPR,qual,TPR/FPR]);

		});
	
		rocs.each(function (n,roc) { 		// update and callback the summary roc
			sql.query("UPDATE rocs SET ? WHERE ?", [{
					FPR: roc.FPR, 
					TPR: roc.TPR, 
					qual: roc.qual
				}, {ID:roc.ID} ] );
			
			if ( res && n==rocs.length-1 && rocs.length >= 5)
				res(roc);
		});
	}

	res(SUBMITTED);
	
	console.log("PRIME DETECTORS");
	
	if ( guardQuery(query,false) ) 	// clear previously locked proofs and retrain selected detectors
		sql.query("UPDATE proofs SET dirty=0,posLock=null,negLock=null WHERE posLock OR negLock", function () {
			sql.query("SELECT * FROM detectors WHERE least(?,1)",query)
			.on("result", function (det) {
				sql.jobs().execute(req.client, det);
			});
		});
	else {	// rescore detectors
		
		/*
		sql.query("SELECT * FROM detectors WHERE Dirty")
		.on("result", function (det) {
			sql.jobs().execute(req.client, det);
			
			sql.query("UPDATE detectors SET Dirty=0 WHERE ?",{ID: det.ID});
		});
		* */

		var 
			pivots = ["detector","channel","model"].concat(flags.pivot || []),
			totNo, totYes, qual, qualfix, f, f0, df, FPR, TPR, Qual = {};
		
		sql.query(
			"SELECT *,concat(??) as category FROM rocs GROUP BY ??,f ORDER BY f DESC" ,
			[ pivots, pivots ], 
			function (err, rocs) {

				if (!err) evalROC( rocs , function (roc) {
		
					sql.query( "SELECT * FROM detectors WHERE least(?,1)", {
						Name: roc.detector,
						Channel: roc.channel
					})
					.on("result", function (det) {

						sql.query("UPDATE detectors SET Stars=round(?,1),Evaled=now() WHERE ?", [
							roc.qual*5/0.5, { ID: det.ID } 
						]);

					});	
					
				});
				
		});
	}
	
	/*
		sql.query("SELECT * FROM detectors WHERE ?", {Name:"default"})
		.on("result", function (defdet) {
			
			delete defdet.ID;
			
			// need to revise to parse the searching text to pull off
			// the aoi to set the det channel
			return;
			
			sql.query("SELECT Searching,Returned FROM searches WHERE Returned = 0 GROUP BY Searching")
			.on("result", function (need) {
				
				sql.query("SELECT Noise FROM files WHERE least(?) GROUP BY Noise", {Area: "NEGATIVES", Signal: need.Searching} )
				.on("result", function (neg) {
					defdet.Name = need.Searching + " in " + neg.Noise;
					defdet.poscases = need.Searching;
					defdet.negcases = neg.Noise;
					detdef.channel = "aoi tbd"; // parse from searching text
					sql.query("INSERT INTO detectors SET ?", defdet);
				});
			
			});

			sql.query("SELECT Signal FROM files WHERE ? GROUP BY Signal", {Area: "POSITIVES"} )
			.on("result", function (pos) {
				
				sql.query("SELECT Noise FROM files WHERE least(?) GROUP BY Noise", {Area: "NEGATIVES", Signal: pos.Signal} )
				.on("result", function (neg) {
					defdet.Name = pos.Signal + " in " + neg.Noise;
					defdet.poscases = need.Searching;
					defdet.negcases = neg.Noise;
					detdef.channel = "none"; 
					sql.query("INSERT INTO detectors SET ?", defdet);	
				});
				
			});

		});
	*/
	
}

SQL.execute.events = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	var	lambdas = (query.lambda||"").parse([1]),
		mus = (query.mu||"").parse([2]),
		runtime = parseFloat(query.runtime||"100"),
		compress = parseFloat(query.compress||"100"),
		correct = query.nocorrect ? false : true,
		label = query.label || "",
		trace = query.trace ? true : false,
		help = query.help || "",
		summary = query.summary || "abs",
		bins = parseInt(query.bins || "50"),
		levels = (query.levels||"").parse(null);
			
	function exptime (rate) { 		// return exponential time deviate whose avg is 1/rate
		return - Math.log(Math.random()) / rate;
	}
	
	function arrival ( ) { 			// generate an arrival using prescribed lambda arrival rate
		
		tStep = exptime(lambda);
		t += tStep;
		var event = { 					// arrival event
				label: label, 			// batch label
				job: n, 				// unique id
				N: n, 				// event counter
				tArrive: t,				// arrival time
				tStep: tStep,			// step time
				tService: exptime(mu)	// assign a service time
		};
		
		sched(tStep, event, function (event, terr) {
				
				Terr += terr;
				
				if (trace)
				console.log(['a'+event.job, Math.round(event.tArrive,4),  Math.round(terr,4), queue.length]);

				if ( ++n < N ) 			// more arrivals to scedule ?
					arrival();
					
				else {						// no more arrivals (there will be straggeling departures)
						if (summary)
						sql.query("select avg(tDelay) as avgDelay, avg(depth) as avgDepth, avg(tStep) as avgStep, avg(tService) as avgService FROM jobs")
						.on("result", function (stat) {
						
						var check = {
							arrivalRate: {expected: lambda, achived: 1/stat.avgStep},
							serviceRate: {expected: mu, achived: 1/stat.avgService},
							utilization: {expected: rho, achived: stat.avgService/stat.avgStep},
							runtime: {expected: T, achived: t},
							avgdelay: {expected: avgdelay, achived: stat.avgDelay},
							avgdepth: {expected: avgdepth, achived: stat.avgDepth},
							samples: {expected: N, achived: n},
							res: {expected: Tres, achived: Tres - Terr/n}
						};
						
						switch (summary) {
							case "abs": 
								var rtns = [];
								for (var idx in {expected:1,achived:1}) {
									var rtn = {stat: idx}; rtns.push(rtn);
									
									for (var n in check) rtn[n] = check[n][idx];
								}
										
								console.log(JSON.stringify(rtns));
								break;
								
							case "rel":
								for (var idx in check) check[idx] = Math.round( (1 - check[idx].expected / check[idx].achived)*100, 4) + "%";
								console.log(JSON.stringify(check));
								break;							
						}
					});

						if (bins) 			// record delay stats or levels
						sql.query("SELECT * FROM jobs WHERE ?", {label:label}, function (err, recs) {

							statJobs(recs, bins, function (stats) {
								
								sql.query("DELETE FROM jobstats WHERE ?", {label:label});
								
								if ( levels )  		// save delay times at prescibed levels of confidence
									levels.each( function (n, level) {
										Each( stats, function (bin, x) {
											if ( (1 - x.pr) < level ) {
												sql.query("INSERT INTO jobstats SET ?", {
													label: label,
													pr: level,
													delay: bin * dT
												});
												return true;
											}
										});
									});
									
								else 					// save delay stats
									Each(stats, function (n,stat) {
										stat.label = label;
										sql.query("INSERT INTO jobstats SET ?", stat);
									});
								
							});
						});
				}
				
				event.tStep += terr;
				event.tArrive += terr;
				queue.push( event );				// add event to the queue

				if ( !head ) service( event.tArrive+terr );		// arrived to idle queue so service
					
			});
	}
	
	function service (t0) { 		// place head of queue into service at time t0
		
		head = queue.shift();												// get head of queue

		head.tWait = t0 - head.tArrive; 							// waiting time in the queue
		head.tDelay = head.tWait + head.tService;		// delay time through the queue
		head.tDepart = t0 + head.tService; 					// departure time

		if (trace)
		console.log(JSON.stringify(head));

		sched(head.tService, head, function (event, terr) {		// service departure
			
			Terr += terr;

			if (trace)
			console.log(['d'+event.job, Math.round(event.tDepart, 4), Math.round(terr,4), queue.length]);
			
			event.tWait -= terr;
			event.tDelay -= terr;
			event.depth = queue.length + 1;
			
			sql.query("INSERT INTO jobs SET ?", event);
			
			if ( queue.length ) 							// when queue not idle ...
					service( event.tDepart ); 				// service next departure from this event
			else 											// queue is idle ...
					head = null;							// nothing in service
					
		});
		
	}
	
	function sched( tdelta , args, res ) { 							// schedule event 
		
		if (correct) {												// use error correction
			var tref = new Date();	
			setTimeout( function (event) { 							// schedule event

				var tnow = new Date(), terr = (tnow - tref) - tdelta; 
				res(args, terr);
				
			}, tdelta);
		}
		else 																			// no error correction
			setTimeout( function (event) { 							// schedule event

				res(args, 0);
				
			}, tdelta);
	}
	
	if (help) 
		res( 
				 "lambda: [list] of event rates\n"
			+	 "mu: [list] of service rates\n"
			+	 "label: add simulation events and stats to specified label area\n"
			+ 	 "bins: number of bins used for event stats (0=no stats)\n"
			+	 "nocorrect: disable event time corrections\n"
			+	 "runtime: desired simulation runtime\n"
			+ 	 "compress: desired time compression\n"
			+ 	 "trace: enable event tracing" 
		);
		
	else {
		res(SUBMITTED);
		
		sql.query("SELECT avg(datdiff(Departed,Arrived)) AS Tsrv FROM queues", [], function (err,stats) {
		sql.query("SELECT datediff(Now(),Arrived) AS Tage FROM queues ORDER BY Arrived ASC", [], function (err,load) {
			
			var Tint = 0;
			for (var n=1,N=load.length; n<N; n++) Tint += load[n].Tage - load[n-1].Tage;
			Tint /= (N-1);
			
			var	lambda = (lambdas[0] || 1/Tint) / compress,  	// compressed arrival rate
				mu = (mus[0] || 1/stats.Tsrv) / compress,		// compressed service rate
				Tres = 10,					// V8 event schedully time resolution [ms]
				Nres = lambda * Tres,		// event tollerance to be kept << 1
				Terr = 0,					// total computing error 
				T = runtime * compress,		// compressed runtime
				rho = lambda / mu,			// utilization
				avgdelay = (1/mu) / (1 - rho),	// expected avg delay
				avgdepth = 1/(1 - rho) , 		// expected avg queue depth
				N = lambda * T,  			// number of events to generate (Little's formula)
				head = null, 				// queue event being serviced (null when queue idle)
				queue = [],					// the queue
				n = 0, 						// arrival event counter
				t = 0;						// arrival event time
				
			arrival(); 						// start arrival process
			
		});
		});
	}
}

SQL.execute.issues = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

SQL.execute.aspreqts = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

SQL.execute.ispreqts = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

// Digital globe interface

SQL.select.EnhancedView = function Select(req, res) {  
	var sql = req.sql, log = req.log, query = req.query;
}

// Alerting Hydra interface

SQL.select.AlgorithmService = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	var args = {		// Hydra parameters
		size: parseFloat(query.SIZE),
		pixels: parseInt(query.PIXELS),
		scale: parseFloat(query.SCALE),
		step: parseFloat(query.STEP),
		range: parseFloat(query.RANGE),
		detects: parseInt(query.DETECTS),
		infile: query.INFILENAME,
		outfile: query.OUTFILENAME,
		channel: query.CHANNEL
	};
	
	// Hydra args dropped in favor of detector parms attached to requested channel
	sql.query("SELECT * FROM detectors WHERE ?", {Channel:query.CHANNEL})
	.on("result", function (det) {
		
		if (SQL.CHIP)
		SQL.CHIP.workflow(sql, {
			detName: det.Name.replace(/ /g,"_"),
			chanName: det.channel,
			size: det.Feature,
			pixels: det.Pixels,
			scale: det.Pack,
			step: det.SizeStep,
			range: det.SizeRange,
			detects: det.Hits,
			infile: det.infile,
			outfile: "/rroc/data/giat/swag/jobs",
			job: {
				client: req.client,
				class: "detect",
				name: det.Name,
				link: det.Name.tag("a",{href:"/swag.jade?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
		
	});

}

/*
SQL.select.NCLalert = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;

}

SQL.select.ESSalert = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;

}
*/

// other

SQL.execute.parms = function Execute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	var parms = {};
	var types = {
		t: "varchar(64)",
		p: "varchar(64)",
		q: "varchar(64)",
		c: "int(11)",
		i: "int(11)",
		a: "int(11)",
		n: "float",
		"%": "float",
		x: "mediumtext",
		h: "mediumtext",
		d: "datetime",
		g: "geometry not null"
	};
	
	function sqlquery(a,b,res) {
		console.log(a+JSON.stringify(b));
		res();
	}

	res(SUBMITTED);
					
	if (true)
	sql.query("SHOW TABLES FROM app1 WHERE Tables_in_app1 NOT LIKE '\\_%'", function (err,tables) {	// sync parms table with DB
		var allparms = {}, ntables = 0;

		console.log("SCANNING TABLES");
		
		tables.each(function (n,table) {
			var tname = table.Tables_in_app1;

			sql.query("DESCRIBE ??",[tname], function (err,parms) {

				parms.each(function (n,parm) {
					var pname = parm.Field.toLowerCase();
					
					if (pname != "id" && pname.indexOf("_")!=0 ) {
						var tables = allparms[pname];
						if (!tables) tables = allparms[pname] = {};
						
						tables[tname] = parm.Type.split("(")[0].replace("unsigned","");
					}
				});

				if (++ntables == tables.length) 
					Each(allparms, function (pname,tables) {
						var tablelist = SQL.listify(tables).join(",");
						var ptype = "varchar";
						for (var tname in tables) ptype = tables[tname];
						
						//console.log([pname,ptype]);
						
						var parm = {Tables:tablelist, Parm:pname, Type:ptype};

						sql.query("INSERT INTO parms SET ? ON DUPLICATE KEY UPDATE ?",[parm,parm]);
					});
			});

		});
	});
	
	if (false)
	sql.query("SELECT * FROM parms", function (err,parms) { // sync DB with parms table
		
		parms.each(function (n,parm) {
			var pname = parm.Parm;
			var ptype = types[parm.Type] || "int(11)";
			var ptables = parm.Tables ? parm.Tables.split(",") : [];
			
			ptables.each( function (n,tname) {
				
				sql.query("ALTER TABLE ?? ADD ?? "+ptype, [tname,pname]);
				
			});
		});
		
	});

}

SQL.execute.roles = function Execute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	var created = [];
		
	res(SUBMITTED);

	sql.query("SELECT * FROM roles")
	.on("result", function (role) {		
		sql.query("CREATE TABLE ?? (ID float unique auto_increment)", role.Table);
	});
}

SQL.execute.lookups = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	// language (l18-abide) lookups
	
	var langs = [  // sourced from "clients/extjs/build/examples/locale/languages.js"
		['af', 'Afrikaans'],
		['bg', 'Bulgarian'],
		['ca', 'Catalonian'],
		['cs', 'Czech'],
		['da', 'Danish'],
		['de', 'German'],
		['el_GR', 'Greek'],
		['en_GB', 'English (UK)'],
		['en', 'English', 'ascii'],
		['es', 'Spanish/Latin American'],
		['fa', 'Farsi (Persian)'],
		['fi', 'Finnish'],
		['fr_CA', 'France (Canadian)'],
		['fr', 'France (France)'],
		['gr', 'Greek (Old Version)'],
		['he', 'Hebrew'],
		['hr', 'Croatian'],
		['hu', 'Hungarian'],
		['id', 'Indonesian'],
		['it', 'Italian'],
		['ja', 'Japanese'],
		['ko', 'Korean'],
		['lt', 'Lithuanian'],
		['lv', 'Latvian'],
		['mk', 'Macedonia'],
		['nl', 'Dutch'],
		['no_NB', 'Norwegian Bokmål'],
		['no_NN', 'Norwegian Nynorsk'],
		['pl', 'Polish'],
		['pt_BR', 'Portuguese/Brazil'],
		['pt_PT', 'Portuguese/Portugal'],
		['ro', 'Romanian'],
		['ru', 'Russian'],
		['sk', 'Slovak'],
		['sl', 'Slovenian'],
		['sr_RS', 'Serbian Cyrillic'],
		['sr', 'Serbian Latin'],
		['sv_SE', 'Swedish'],
		['th', 'Thailand'],
		['tr', 'Turkish'],
		['ukr', 'Ukrainian'],
		['vn', 'Vietnamese'],
		['zh_CN', 'Simplified Chinese'],
		['zh_TW', 'Traditional Chinese']
	];

	res(SUBMITTED);
	
	langs.each(function (n,lang) {
		sql.query("INSERT INTO lookups SET ?",{
			Enabled: 1,
			Ref: "language",
			Name: lang[1],
			Path: lang[0]
		});
	});

	// extjs theme lookups
	
	var root = '/clients/extjs/packages/';
	var themes = [
		"aria", "classic", "classic-sandbox", "crisp", "crisp-touch", "gray", "neptune", "neptune-touch", "neutral"
	];

	themes.each(function (n,theme) {
		sql.query("INSERT INTO lookups SET ?",{
			Enabled: 1,
			Ref: "theme",
			Name: theme,
			Path: theme //root + theme + '/build/resources/' + theme + "-all.css"
		});
	});
	
	// swag operating setpoints (detector policies)
	
	var setpoints = {
		low: "/config.sys?SetPoint=low",
		medium: "/config.sys?SetPoint=medium",
		high: "/config.sys?SetPoint=high"
	};
	
	Each(setpoints, function (point,path) {
		sql.query("INSERT INTO lookups SET ?",{
			Enabled: 1,
			Ref: "setpoint",
			Name: point,
			Path: path
		});
	});
	
	/*
	// pdf (pdffiller) form translations
	
	var forms = {test1040: "public/uploads/test1040.pdf"};
	
	for (var form in forms) 
		PDF.generateFDFTemplate(forms[form], null, function (err, fields) {
			for (var field in fields) 
				sql.query("INSERT INTO lookups SET ?", {
					Enabled: 1,
					Ref: form,
					Name: "tbd",
					Path: field
				});
		}); */
}

SQL.execute.searches = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);

	sql.query("SELECT * FROM config")
	.on("result", function (config) {
		sql.query(
			"SELECT * FROM searches "
			+ "LEFT JOIN openv.profiles ON openv.profiles.Client = searches.Client AND openv.profiles.QoS>=? "
			+ "WHERE Count>=? AND Returned<=? "
			+ "HAVING datediff(now(),made)<= ?"
			+ "ORDER BY openv.profiles.QoS,openv.profiles.Client" , [1,1,0,30]
		).on("error", function (err) {
			console.log("search apperr="+err);
		}).on("result", function (make) {
			if (make.ID)
				sql.query("SELECT * FROM tempdets WHERE ? LIMIT 1",{Name:config.SetPoint})
				.on("result", function (temp) {
					delete temp.ID;
					temp.Name = make.Searching;
					temp.Client = req.client;
					temp.PosCases = make.Searching + "%";
					temp.NegCases = "%";
					sql.query("INSERT INTO detectors SET ?",temp);
				});
		});
	});
}
					
SQL.execute.chips = function Execute(req, res) {   
	// flush old/bad chips. prime chip address
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	
	// flush - reserved
	
	// prime address
	for (var n=0,N=1e5;n<N;n++) {
		var x = [Math.random(),Math.random(),Math.random(),Math.random()],
			y = [Math.random(),Math.random(),Math.random(),Math.random()];
			
		sql.query(
			"INSERT INTO chips SET ?,address=geomfromtext(?)", [
			{ name: "bogus" }, 
			'POLYGON((' + [
				[x[0],y[0]].join(" "),
				[x[1],y[1]].join(" "),
				[x[2],y[2]].join(" "),
				[x[3],y[3]].join(" "),
				[x[0],y[0]].join(" ") ].join(",")
				+'))'
			]);
	}
				
}

SQL.execute.swaps = function Execute(req, res) {   
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);

	var form_name = "test1040",
		form_template = "~/sigma/shares/" + form_name + ".pdf";

	// poll repos for new packages
	
	sql.query("SELECT * FROM swaps WHERE Enabled AND least(?,1) GROUP BY SW_Name", guardQuery(query,true) )
	.on("result", function (swap) {

		var	package = swap.SW_Name,
			type = swap.Install_URL.filetype(".web"),
			dest = swap.Install_Path.replace("~/","~/test/"),
			install = "~/test/installs/" + package + type,
			form_final = "~/test/sigma/shares/" + package + ".pdf",
			approv = "~/test/sigma/shares/" + package + ".msg",
			packers = {
				".zip": "unzip $I -d $D",
				".tar": "tar -C $D -xvf $I"
			};

		console.log("INSTALLING "+package);

		// submit swaps that have not already been submitted 
		
		sql.query("SELECT * FROM swaps WHERE Enabled AND NOT Submitted")
		.on("result", function (swap) {
			sql.query("UPDATE swaps SET ? WHERE ?",[{Submitted: new Date()}, {ID:swap.ID}]);
			
			sendMail({
				from: SITE.POC,
				to:  SITE.POC,
				subject: "SWAP "+package,
				html: "Please find attached SWAP.  Delivery of  "+swap.Product+" is conditional on NGA/OCIO acceptance of this deviation request. ",
				alternatives: [{
					contentType: 'text/html; charset="ISO-59-1"',
					contents: ""
				}]
			}, function (err,info) {
				if (err) console.log(`EMAIL ${err}`);
			});
			
		});
		
		return;
		
		// create a swap pdf
		
		sql.query("SELECT * FROM lookups WHERE ?",{Ref:form_name}, function (err,looks) {
			
			var form_map = {};
			
			looks.each(function (n,look) {
				if (look.Name)
					form_map[look.Name] = look.Path;
			});
				
			var swap_map = {};
			
			for (var n in form_map)
				swap_map[form_map[n]] = swap[n];

			/*PDF.fillForm( form_template, form_final, swap_map, function (err) {
				if (err) console.log(`SWAP ${package} err`);
			});*/
			
			/*
			PDF.mapForm2PDF( swap, form_conv, function (err,swap_map) {
				console.log("map="+err);
				
				PDF.fillForm( form_template, form_final, swap_map, function (err) {
					console.log("fill="+err);
				});
			});
			* */

		});
		
		// archive old package and get new package
		
		'mv $I ~/test/installs/archive'.command({I:install}, "", function () {
		'wget "$U" -O $I'.command({U:swap.Install_URL,I:install}, "", function () {
			
			var unpack = packers[type];
			
			if (unpack)
				unpack.command({I:install,D:dest}, "", function () {
					console.log("UNPACKED "+package);
				});
			else {  // crawl site with phantomjs to pull latest source
			}

		});
		});
	});
	
}

SQL.execute.hawks =
SQL.execute.jobs = function Execute(req, res) {
/*
 * Hawk over jobs in the queues table given {Action,Condition,Period} rules 
 * defined in the hawks table.  The rule is run on the interval specfied 
 * by Period (minutes).  Condition in any valid sql where clause. Actions 
 * supported:
 * 		stop=halt=kill to kill matched jobs and update its queuing history
 * 		remove=destroy=delete to kill matched jobs and obliterate its queuing history
 * 		log=notify=flag=tip to email client a status of matched jobs
 * 		improve=promote to increase priority of matched jobs
 * 		reduce=demote to reduce priority of matached jobs
 * 		start=run to run jobs against dirty records
 * 		set expression to revise queuing history of matched jobs	 
 * */

	var sql = req.sql, log = req.log, query = req.query;
	
	function hawkjobs (sql, client)  {

		function hawk(rule) {
			SQL.thread(function (sql) {

				console.log(`HAWK ${rule.Action} WHEN ${rule.Condition} EVERY ${rule.Period} mins`);
				
				sql.query("UPDATE hawks SET Pulse=Pulse+1 WHERE ?", {ID:rule.ID});

				switch (rule.Action.toLowerCase()) {
					case "scrape":
					case "execute":
						
						SQL.CRUDE({
							query: {},
							body: {},
							param: function () { return ""; }
							/*connection: {
								listeners: function () {return "";},
								on: function () {return this;},
								setTimeout: function () {}
							}*/
						}, {
							send: function (rtn) {console.log(rtn);} 
						}, rule.Table, "execute");
						
						break;
						
					case "stop":
					case "halt":
					case "delete":
					case "kill":

						sql.jobs().delete(rule.Condition, function (job) {
							sql.query("UPDATE hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
						});
					
						break;
						
					case "log":
					case "notify":
					case "tip":
					case "warn":
					case "flag":
					
						sql.jobs().select(rule.Condition, function (job) { 

							sql.jobs().update({ID:job.ID}, { 	// reflect history
								Flagged: 1,
								Notes: "Clear your Flagged jobs to prevent these jobs from being purged"
							}, function () {
								
								sendMail({
									from: SITE.POC,
									to:  job.Client,
									subject: SQL.SITE.title + " job status notice",
									html: "Please "+"clear your job flag".tag("a",{href:"/rule.jade"})+" to keep your job running.",
									alternatives: [{
										contentType: 'text/html; charset="ISO-59-1"',
										contents: ""
									}]
								}, function (err,info) {
									if (err) console.log(`EMAIL ${err}`);
								});

							});
						});
						
						break;
						
					case "promote":
					case "improve":

						sql.jobs().update(rule.Condition, +1, 0, function (job) {
							sql.query("UPDATE hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
						});
						
						break;
						
					case "demote":
					case "reduce":
					
						sql.jobs().update(rule.Condition, -1, 0, function (job) {
							sql.query("UPDATE hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
						});
						
						break;
						
					case "start":
					case "run":
					
						sql.query(
							// "SELECT detectors.* FROM detectors LEFT JOIN queues ON queues.Job = detectors.Name WHERE length(NegCases) AND "+rule.Condition)
							"SELECT * FROM ?? WHERE "+rule.Condition, 
							rule.Table
						).on("result", function (det) {
							//det.Execute = "resample";
							//console.log(det);
							sql.query("UPDATE ?? SET Dirty=0 WHERE ?",[rule.Table,{ID: det.ID}]);
							sql.jobs().execute(client,det);
						});
						break;
					
					case "":
					case "skip":
					case "ignore":
						
						break;
						
					default:
						console.log(rule.Action);
						
				}
				
				sql.release();
			});
		}
				
		SQL.timers.each( function (n,id) { 	// kill existing hawks
			clearInterval(id);
		});
		
		SQL.timers = [];
		
		//sql.query("DELETE FROM queues"); 		// flush job queues
		
		sql.query("SELECT * FROM config WHERE Hawks") 			// get hawk config options
		.on("result", function (config) {
			sql.query(
				"SELECT * FROM hawks WHERE least(?) AND Faults<?  AND `Condition` IS NOT NULL", 
				[{Enabled:1,Name:config.SetPoint},config.MaxFaults]
			).on("result", function (rule) { 		// create a hawk
				if (rule.Period) 					// hawk is periodic
					SQL.timers.push( 
						setInterval( hawk, rule.Period*60*1000, rule )
					);
				else 								// one-time hawk
					hawk(rule);
			});
		
		});
	}

	res(SUBMITTED);
	
	hawkjobs(sql, req.client);
}

// Job queue CRUDE interface

SQL.select.jobs = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	// route valid jobs matching sql-where clause to its assigned callback res(job).
	
	Trace( sql.query(
		"SELECT *, datediff(now(),Arrived) AS Age,openv.profiles.* FROM queues LEFT JOIN openv.profiles ON queues.Client=openv.profiles.Client WHERE NOT Hold AND Departed IS NULL AND NOT openv.profiles.Banned AND least(?,1) ORDER BY queues.QoS,queues.Priority", 
		guardQuery(query,true), function (err, recs) {
			res( err || recs );
	}));
	
}

SQL.update.jobs = function Update(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
	
	// adjust priority of jobs matching sql-where clause and route to callback res(job) when updated.
		
	if (protect)
		res( SQL.errors.protectedQueue );
	else
		sql.query("UPDATE queues SET ? WHERE ?",body,query, function (err,info) {
			res( err || info );
		});
}
		
SQL.delete.jobs = function Delete(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
		
	if (protect)
		res( SQL.errors.protectedQueue );
	else
		sql.query("DELETE FROM queues WHERE ?",query, function (err,info) {
			res( err || info );
		});
}

SQL.insert.jobs = function Insert(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
		
	if (protect)
		res( SQL.errors.protectedQueue );
	else
		sql.query("INSERT INTO queues SET ?",body, function (err,info) {
			res( err || info );
		});
}
	
// JSON editor

SQL.select.json = 
SQL.update.json = 
SQL.delete.json = 
SQL.insert.json = 

function (req,res) {
	
	var sql = req.sql,
		query = req.query,
		body = req.body,
		flags = req.flags,
		keys = (query.key||body.key||"nokey").split("."),
		db = "openv." + keys[0];

	delete query.key;  //$$$$
	delete body.key;
	
console.log({
	q: query,
	f: flags,
	d: db,
	k: keys,
	b: body});
			
	sql.query(
		"SELECT ??,ID,count(ID) as Counts FROM ?? WHERE ? LIMIT 0,1", 
		[keys[2] || "ID", db, {Nick: keys[1]}])
	
	.on("error", function (err) {
		res(err);
	})
	
	.on("result", function (dbInfo) {

		if (dbInfo.Counts) {
			
			var rtns = [], Info = (dbInfo[keys[2]] || "").parse({}), ID = 0;
			
			for (recs=Info,n=3,N=keys.length; n<N; n++) 
				if (val = recs[keys[n]])
					recs = val;
				else
					recs = recs[keys[n]] = [];

			if (recs.constructor == Array) {
				recs.each(function (i,rec) {
					
					var match = true;
					for (var x in query) 
						if (query[x] != rec[x]) match = false;
						
					if (rec.ID > ID) ID = rec.ID;
					if (match) rtns.push( rec );
					
				});
		
				switch (req.action) {
					case "select": 
						
						res(rtns);
						break;
						
					case "update": 
						
						rtns.each( function (i,rec) {
							for (var x in body) rec[x] = body[x];
						});
						
						sql.query("UPDATE ?? SET ? WHERE ?", [
							db,
							{Info: JSON.stringify(Info)},
							{ID:dbInfo.ID}
						]);
						
						res({});
						break;
						
					case "delete":

						for (var j=k=0; j<recs.length && k<rtns.length; )
							if (recs[j] == rtns[k]) {
								recs.splice(j,1);
								k++;
							}
							else
								j++;
								
						sql.query("UPDATE ?? SET ? WHERE ?", [
							db,
							{Info: JSON.stringify(Info)},
							{ID:dbInfo.ID}
						]);
						
						res({});
						break;

					case "insert":
						var rec = {ID: ID+1};
						for (var x in query) rec[x] = query[x];
						for (var x in body) rec[x] = body[x];
						
						recs.push(rec);

						sql.query("UPDATE ?? SET ? WHERE ?", [
							db,
							{Info: JSON.stringify(Info)},
							{ID:dbInfo.ID}
						]);
						
						res({insertId: rec.ID});
						break;
				}

			}
			else
				res(recs);
			
		}
		else 
			res([]);

	});
	
}
	
// VTL for detectors table

SQL.execute.detectors = function Execute(req, res) { 
// execute(client,job,res): create detector-trainging job for client with callback to res(job) when completed.

	var sql = req.sql, log = req.log, query = req.query, job = req.body;

	var vers = [0]; //job.Overhead ? [0,90] : [0];
	var labels = job.Labels.split(",");

console.log(">>>>>>>>>>>>>>>>>>>>> labels="+labels);

	// train classifier
	//	`python ${ENV.CAFENGINES}/train`

	// train locator
	
	labels.each(function (n,label) {
		
		var posFilter = "digit +" + label,
			newFilter = "digit -" + label;
		
		sql.query(		// lock proofs
			"START TRANSACTION", 
			function (err) {	

		sql.query( 		// allocate positives to this job
			"UPDATE proofs SET ? WHERE ? AND ?",
			[{posLock:job.Name}, {cat:"digit"}, {label:label}],
		//	"UPDATE proofs SET ? WHERE MATCH (label) AGAINST (? IN BOOLEAN MODE) AND enabled",
		//	[{posLock:job.Name},posFilter], 
			function (err) {

		sql.query(		// allocate negatives to this job
			"UPDATE proofs SET ? WHERE ? AND NOT ?",
			[{negLock:job.Name}, {cat:"digit"}, {label:label}],
		//	"UPDATE proofs SET ? WHERE MATCH (label) AGAINST (? IN BOOLEAN MODE) AND enabled",
		//	[{negLock:job.Name},negFilter], 
			function (err) {

		sql.query(
			"SELECT * FROM proofs WHERE ? LIMIT 0,?",		// get allocated positives
			[{posLock:job.Name},job.MaxPos],
			function (err,posProofs) {

		sql.query(								// get allocated negatives
			"SELECT * FROM proofs WHERE ? LIMIT 0,?",
			[{negLock:job.Name},job.MaxNeg],
			function (err,negProofs) {
				
		sql.query(			// end proofs lock.
			"COMMIT", 
			function (err) { 
		
console.log([">>>> proofs",posProofs.length,negProofs.length]);

		if (posProofs.length && negProofs.length) {	// must have some proofs to execute job
			
			var	posDirty = posProofs.sum("dirty"),
				negDirty = negProofs.sum("dirty"),
				totDirty = posDirty + negDirty,
				totProofs = posProofs.length + negProofs.length,
				dirtyness = totDirty / totProofs;

			console.log('DIRTY CHECK '+[dirtyness,job.MaxDirty,posDirty,negDirty,posProofs.length,negProofs.length]);

			sql.query("UPDATE detectors SET ? WHERE ?",[{Dirty:dirtyness},{ID:job.ID}]);
			
			if (dirtyness >= job.MaxDirty) {		// sufficiently dirty to cause job to execute ?
				
				sql.query("UPDATE proofs SET dirty=0 WHERE least(?)",{posLock:job.Name,negLock:job.Name});
				
				vers.each( function (n,ver) {  		// train all detector versions
						
					var det = SQL.clone(job);
					
					det.Path = "det"+ver+"/"+label+"/"; 		// detector training results placed here
					det.DB = "../db"+ver;						// positives and negatives sourced from here relative to ENV.DETS
					det.posCount = posProofs.length;
					det.negCount = negProofs.length;
					det.posPath = det.Path + "positives.txt"; 	// + ENV.POSITIVES + (false ? jobFolder + ".positives" : det.PosCases + ".jpg");  		// .positives will disable auto-rotations
					det.negPath = det.Path + "negatives.txt"; 	// + ENV.NEGATIVES + jobFolder + ".negatives";
					det.vecPath = det.Path + "samples.vec";
					det.posLimit = Math.round(det.posCount * 0.9); 	// adjust counts so haar trainer does not exhaust supply
					det.negLimit = Math.round(det.negCount * 1.0);
					
					det.link = det.Name.tag("a",{href:"/swag.jade?goto=Detectors"}) + " " + det.posLimit + " pos " + det.negLimit + " neg";
					det.name = det.Name;
					det.client = log.client;
					det.work = det.posCount + det.negCount;

					console.log("TRAIN "+det.Name+"["+ver+"]");
				
					var Execute = {
						Purge: "rm -rf " + det.Path,
						Reset: "mkdir -p " + det.Path,
						
						// ************* NOTE 
						// ****** Must pass bgcolor and bgthres as parms too - positive dependent
						// ****** so must be dervied from image upload tags
						Resample: 
							`opencv_createsamples -info ${det.posPath} -num ${det.posCount} -w ${det.Width} -h ${det.Height} -vec ${det.vecPath}`,
							//"opencv_createsamples -info $posPath -num $posCount -w $Width -h $Height -vec $Data/samples.vec",
							//"opencv_createsamples $Switch $posPath -bg $negPath -vec $Vector -num $Samples -w $Width -h $Height -bgcolor 112 -bgthresh 5 -maxxangle $xRotate -maxyangle $yRotate -maxzangle $zRotate -maxidev $ImageDev",

						Train: 
							`opencv_traincascade -data ${det.Path} -vec ${det.vecPath} -bg ${det.negPath} -numPos ${det.posLimit} -numNeg ${de.negLimit} -numStages ${det.MaxStages} -w ${det.Width} -h ${det.Height} -featureType LBP -mode BASIC`
							//"opencv_traincascade -data $Cascade -bg $negPath -vec $Vector -numPos $Positives -numNeg $Negatives -numStages $MaxStages -precalcValBufSize 100 -precalcIdxBufSize 100 -featureType HAAR -w $Width -h $Height -mode BASIC -minHitRate $MinTPR -maxFalseAlarmRate $MaxFPR -weightTrimRate $TrimRate -maxDepth $MaxDepth -maxWeakCount $MaxWeak"										
					};
						
					console.log((det.Execute||"").toUpperCase()+" "+det.name);
					
					/**
					* Training requires:
					*  	SAMPLES >= POSITIVES + (MAXSTAGES - 1) * (1 - STAGEHITR) * POSITIVES + NEGATIVES
					* that is:
					*	POSITIVES <= (SAMPLES-NEGATIVES) / (1 + (MAXSTAGES-1)*(1-STAGEHITR))
					*
					* Actual STAGES (from training log) <= MAXSTAGES 
					* Desired HITRATE = STAGEHITR ^ MAXSTAGES --> STAGEHITR ^ (Actual STAGES)
					* Desired FALSEALARMRATE = STAGEFAR ^ MAXSTAGES --> STAGEFAR ^ (Actual STAGES)
					*
					* The samples_zfullN100 file will always contain $NEGATIVES number of negative images.
					*/
								
					switch (det.Execute.toLowerCase()) {
						case "purge": 
						case "clear":
							//sql.jobs().insert( "purge", Execute.Purge, det);
							break;
							
						case "reset":
						case "retrain":
							
							if (true) {						// gen training positives
								var list = []; 

								posProofs.each( function (n,proof) {
									//list.push(proof.Name + " 1 0 0 " + (proof.Width-1) + " " + (proof.Height-1) );
									list.push([det.DB+"/"+proof.name, 1, proof.left, proof.top, proof.width, proof.height].join(" "));
								});
								
								FS.writeFileSync(
									`./public/dets/${det.posPath}`, 
									list.join("\n")+"\n","utf-8");
							}

							if (true) {					 	// gen training negatives
								var list = [];

								negProofs.each( function (n,proof) {
									list.push(det.DB+"/"+proof.name);
								});

								FS.writeFileSync(
									`./public/dets/${det.negPath}`, 
									list.join("\n")+"\n","utf-8");
							}

							if (true)
								sql.jobs().insert( "reset", Execute.Reset, det, function () {
									sql.jobs().insert( "sample", Execute.Resample, det, function () {
										sql.jobs().insert( "learn", Execute.Train, det, function () {
											if (res) res(det);
										});
									});
								});
								
							break;
							
						case "resample":
						
							sql.jobs().insert( "sample", Execute.Resample, det, function () {
								sql.jobs().insert( "learn", Execute.Train, det, function () {
									if (res) res(det);
								});
							});
							break;
							
						case "transfer":
						
							sql.jobs().insert( "learn", Execute.Train, det, function () {
								if (res) res(det);
							});
							break;
							
						case "baseline":
							break;
							
						case "run":
						case "detect":

							if (SQL.CHIP)
							SQL.CHIP.workflow(sql, {
								detName: det.Name.replace(/ /g,"_"),
								chanName: det.Channel,
								size: det.Feature,
								pixels: det.Pixels,
								scale: det.Pack,
								step: det.SizeStep,
								detects: det.Hits,
								infile: det.infile,
								outfile: "/rroc/data/giat/swag/jobs",
								job: {
									client: req.client,
									class: "detect",
									name: det.Name,
									link: det.Name.tag("a",{href:"/swag.jade?goto=Detectors"}),
									qos: req.profile.QoS,
									priority: 1
								}									
							});
							
							break;
					}

				});
				
			}
		}
		
		}); // commit proofs
		}); // select neg proofs
		}); // select pos proofs
		}); // update neg proofs
		}); // update pos proofs
		}); // lock proofs
	
	});	// labels
}

/**
 * @private
 * 
 * Private functions
 * */		

function guardQuery(query, def) {
	for (var n in query) return query;
	return def;
}

function statJobs(recs, bins, res) {

	var hist = new Array(bins), stats = new Array(bins);
	var minDelay = 9e99, maxDelay = -1;
	var N = recs.length;

	for (var n=0; n<N; n++) {
			var rec = recs[n];
			if ( rec.tDelay < minDelay) minDelay = rec.tDelay;
			if ( rec.tDelay > maxDelay) maxDelay = rec.tDelay;
	}
	
	var dT = (maxDelay - minDelay) / bins;
	
	for (var n=0; n<bins; n++) hist[n] = 0;
	
	for (var n=0; n<N; n++) 
		hist[ Math.round( (recs[n].tDelay - minDelay) / dT ) ]++;
	
	for (var n=0, scale = N*dT; n<bins; n++) hist[n] /= scale;

	for (var n=0, sum = hist[0]*dT; n<bins; n++,sum+=hist[n]*dT) stats[n] = sum;
	
	for (var n=0; n<bins; n++) stats[n] = { delay: n*dT , pr: stats[n] };
	
	res( stats );
}

function statRepo(sql) {
	var lookups = {
		issues: "Action",
		default: "Special"
	};		
		
	console.log("REFLECT GIT LOGS");
	
	CP.exec('git log --reverse --pretty=format:"%h||%an||%ce||%ad||%s" > gitlog', function (err,log) {
			
		FS.readFile("gitlog", "utf-8", function (err,logs) {
			
			logs.split("\n").each( function (n,log) {
				
				var	parts = log.split("||"),
					info = {	
						hash: parts[0], 
						author: parts[1],
						email: parts[2],
						made: new Date(parts[3]),
						cm: parts[4]
					};
					
				var	tags = info.cm.split("#"),
					cm = tags[0];
				
				if ( tags.length == 1 ) tags = tags.concat("issues 116");
				
				for (var n=1,N=tags.length; n<N; n++) { 	// update tagged tables
					
					var	parts = tags[n].split(" "),
						note = { },
						table = parts[0];
						
					note[ lookups[table] || lookups.default ] = cm + " on " + info.made + " by " + info.author.tag("a",{href:"mailto:"+info.email});

					for (var k=1,K=parts.length; k<K; k++) {
						var id = parts[k];

						if (id)
							sql.query("UPDATE openv.?? SET ? WHERE ?", [
								table, 
								note,
								parseInt(id) ? { ID: id } : { Num: id }
							]);
					}
				}
				
			});
		});		
	});
	
}

function compileEngine(engine,name,code,res) {
	try {
		VM.runInContext( "SQL."+engine+"."+name+"="+code, VM.createContext({SQL:SQL})  );
		
		if (res) res("ok");
	}
	catch (err) {
		if (res) res(new Error(err+""));
	}
}			
	
function feedNews(sql,engine) {
	sql.query("SELECT * FROM features WHERE NOT ad")
	.on("result", function (feature) {
		NEWSFEED.addItem({
			title:          feature.feature,
			link:           `${SQL.URL.HOST}/feed.jade`,
			description:    JSON.stringify(feature),
			author: [{
				name:   SQL.SITE.title,
				email:  SQL.EMAIL.SOURCE,
				link:   SQL.URL.HOST
			}],
			/*contributor: [{
				name:   SQL.TITLE,
				email:  site.EMAIL.SOURCE,
				link:   SQL.URL.HOST
			}],*/
			date:           feature.found
			//image:          posts[key].image
		});
	});	
    
	NEWSFEED.render("rss-2.0");  // or "atom-1.0"

	sql.query("UPDATE features SET Ad=1 WHERE NOT ad");
    
	READ(SQL.NEWREAD.URL, function(err, articles) {
		if (err)
			console.info("Ignoring news reader "+SQL.NEWREAD.URL);
		else
			articles.each( function (n,article) {
				// "title"     - The article title (String).
				// "author"    - The author's name (String).
				// "link"      - The original article link (String).
				// "content"   - The HTML content of the article (String).
				// "published" - The date that the article was published (Date).
				// "feed"      - {name, source, link}
				
				// if this is a foreign feed, we can set args to ("change", associated file)
				if (SQL.NEWREAD.JOB)
					SQL.NEWREAD.JOB( sql, "feed", article.link);
			});
	});
}

/*
 * @method requestService(srv,cb)
 * 
 * Issue http/https request to the desiired service srv (host,port,path) using 
 * the specified srv method. The response is routed to the callback cb.
 * Known nodejs bug prevents connections to host = "localhost".
 * */
function requestService(srv, cb) {

	var req = HTTP.request(srv, function(cb) {
		
		console.log(`STATUS: ${cb.statusCode}`);
		console.log(`HEADERS: ${JSON.stringify(cb.headers)}`);
		
		cb.setEncoding('utf8');

		var body = "";
		cb.on('data', function (chunk) {
			body += chunk;
		});
		
		cb.on("end", function () {
			
			var recs = body.parse({data:[]});

			recs.data.each( function (n,rec) {
				cb(rec);
			});			
		});
	});

	req.on('error', function(err) {
		console.log("http apperr="+err);
	});

	//req.write('data\n');	// write data to request body
	req.end();				// end request
}

// Email support methods

function showIMAP(obj) {
//  return inspect(obj, false, Infinity);
}

function killIMAP(err) {
	console.info(ME+'Uh oh: ' + err);
	process.exit(1);
}

function openIMAP(cb) {
	SQL.EMAIL.RX.TRAN.connect(function(err) {  // login cb
		if (err) killIMAP(err);
		SQL.EMAIL.RX.TRAN.openBox('INBOX', true, cb);
	});
}

function sendMail(opts, cb) {
	
	if (SQL.EMAIL.TRACE) console.log(`>>Emailing ${opts.to} ${opts.subject}`);
		
	if (x = SQL.EMAIL)
		if (x = x.TX.TRAN)
			x.sendMail(opts,cb);
}

// UNCLASSIFIED

