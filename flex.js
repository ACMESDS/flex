// UNCLASSIFIED

/*
@class flex
@requires vm
@requires http
@requires crypto
@requires url
@requires cluster
@requires fs
@requires child-process
@requires os

@requires enum
@requires engine
@requires jslab

@requires pdffiller
@requires nodemailer
@requires nodemailer-smtp-transport
@requires imap
@requires feed
@requires feed-read
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
	RAN = require("randpr"), 		// random process
	FEED = require('feed');				// RSS / ATOM news feeder
	//READ = require('feed-read'); 		// RSS / ATOM news reader

var 						// globals
	ENV = process.env, 					// external variables
	SUBMITTED = "submitted";

var 											// totem bindings
	READ = require("reader"),
	JSLAB = require("jslab"),
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
	Each = ENUM.each,
	Log = console.log;

var
	FLEX = module.exports = {
		
		copy: Copy,
		each: Each,
		
		// Job hawking  etc
		timers: [],
		sendMail: sendMail,
		
		errors: {
			badbaseline: new Error("baseline could not reset change journal"),
			disableEngine: new Error("requested engine must be disabled to prime"),
			noEngine: new Error("requested engine does not exist"),
			missingEngine: new Error("missing engine query"),
			protectedQueue: new Error("action not allowed on this job queues"),
			noCase: new Error("plugin case not found"),
			badAgent: new Error("agent failed"),
			badDS: new Error("dataset could not be modified"),
			badLogin: new Error("invaid login name/password"),
			failedLogin: new Error("login failed - admin notified")
		},
		
		attrs: {  // static table attributes (e.g. geo:"fieldname" to geojson a field)
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

		diag: { // configured for system health info
		},
		
		paths: {
			newsread: "http://craphound.com:80/?feed=rss2",
			aoiread: "http://omar.ilabs.ic.gov:80/tbd",
			host: ""
		},
			
		/*
		billingCycle: 0,  // job billing cycle [ms] ( 0 disables)
		diagCycle: 0, // self diagnostic cycle [ms] (0 disables)
		hawkingCycle: 0, // job hawking cycle [ms] (0 disables)
		*/
		
		flatten: { 		// catalog flattening 
			catalog: {  // default tables to flatten and their fulltext keys
				files: "Tag,Area,Name",
				intake: "Special,Name,App,Tech",
				engines: "Code,engine,Classif,Special",
				news: "Message",
				sockets: "Location,Client,Message",
				detects: "label"
			},
			
			limits: {  // flattening parameters
				score: 0.1,
				records: 100,
				stamp: new Date(),
				pivots: ""
			}
		},
		
		dbRoutes: {  //< default table -> db.table translators
		},
		
		// CRUDE interface
		select: {ds: queryDS}, 
		delete: {ds: queryDS}, 
		update: {ds: queryDS}, 
		insert: {ds: queryDS}, 
		execute: {ds: queryDS}, 
	
		fetcher: null, 					// http data fetcher
		uploader: null,		 			// file uploader
		emitter: null,		 			// Emitter to sync clients
		thread: null, 					// FLEX connection threader
		skinner : null, 				// Jade renderer
		
		runEngine: function (req,res) {  // run engine and callback res(ctx || null) with updated context ctx
			
			ENGINE.select(req, function (ctx) {  // compile and step the engine
				//Log("run eng", ctx);
				
				if (ctx)  
					res( ctx );
				
				else  { // engine does not exist/failed so try builtin
					if ( plugin = FLEX.plugins[req.table] )  
						
						try {
							plugin(req.query, res);
						}
						
						catch (err) {
							res( null );
						}

					else
						res( null );
				}
			});
		},
		
		eachPlugin: function ( sql, group, cb ) {  // callback cb(eng,ctx) with each engine and its context meeting ctx where clause
			sql.eachTable( group, function (table) { 
				ENGINE.getEngine( sql, group, table, function (eng) {
					if (eng) cb(eng);
				});
			});
		},
		
		eachUsecase: function ( sql, group, where, cb ) {  // callback cb(eng,ctx) with each engine and its context meeting ctx where clause
			FLEX.eachPlugin( sql, group, function (eng) {
				if (eng)
					FLEX.getContext( sql, group+"."+table, where, function (ctx) {
						if (ctx) cb( eng, ctx );
					});
			});
		},
		
		/*
		taskPlugins: function ( sql, group, cb ) {  //< callback cb(taskID,pluginName) of cloned ingest usecase
			
			if (false) // legacy - clone Name="ingest" mode
				FLEX.eachUsecase( sql, group, {Name:"ingest"}, function (eng,ctx) {
					var tarkeys = [], srckeys = [], hasJob = false, pluginName = eng.Name;

					Object.keys(ctx).each( function (n,key) { // look for Job key
						var keyesc = "`" + key.Field + "`";
						switch (key.Field) {
							case "Save":
								break;
							case "Job":
								hasJob = true;
							case "Name":
								srckeys.push("? AS "+keyesc);
								tarkeys.push(keyesc);
								break;
							default:
								srckeys.push(keyesc);
								tarkeys.push(keyesc);
						}
					});

					if (hasJob) 
						sql.query( // add usecase to plugin by cloning its Name="ingest" usecase
							"INSERT INTO ??.?? ("+tarkeys.join()+") SELECT "+srckeys.join()+" FROM ??.?? WHERE ID=?", [
								group, pluginName,
								"ingest " + new Date(),
								JSON.stringify(job),
								group, pluginName,
								ctx.ID
						], function (err, info) {
							if ( !err ) cb( pluginName, info.insertId );
						});
				});
			
			else 
				FLEX.eachPlugin( sql, group, function (eng) {
					if (eng) cb( eng.Name );
				});
		},
		*/
		
		getContext: function ( sql, ds, where, cb ) {  //< callback cb(ctx) with primed context or null
			
			function config(js, ctx) {
				//Log("config", js);
				try {
					VM.runInContext( js, VM.createContext(ctx)  );	
				}
				catch (err) {
					Log(err);
				}
				//Log("ctx",ctx);
			}
			
			sql.first("", "SELECT * FROM ?? WHERE least(?,1) LIMIT 1", [ds,where], function (ctx) {
				
				if (ctx) {
					if ( ctx.Config ) config(ctx.Config, ctx);

					sql.jsonKeys( ds, [], function (keys) {  // parse json keys
						//Log("json keys", keys);
						keys.each(function (n,key) {
							//Log(key, key.indexOf("Save") );
							if ( key.indexOf("Save")<0 )
								try { 
									ctx[key] = JSON.parse( ctx[key] || "null" ); 
								}

								catch (err) {
									ctx[key] = null; 
								}

							else
								ctx[key] = null;
						});
						
						cb(ctx);
						//Log("get ctx",ctx);						
					});
				}

				else
					cb(ctx);			
			});
			
			/*
			var 
				id = ctx.id || ctx.ID,
				name = ctx.name || ctx.Name,
				dsquery = id ? {ID: id} : name ? {Name: name} : null;
			
			if (dsquery)  // get context from dataset
				sql.query( "SELECT * FROM ?? WHERE ? LIMIT 0,1", 	[ds, dsquery], function ( err, ctxs ) {						
					if (err) 
						cb( null );
						
					else 
					if ( isEmpty = ctxs.each() )
						cb( null );
					
					else {
						Copy( ctxs[0], ctx );
						
						sql.jsonKeys( ds, function (keys) {  // parse json keys
							keys.each(function (n,key) {
								if ( key.indexOf("Save")<0 )
									try { 
										ctx[key] = JSON.parse( ctx[key] ); 
									}

									catch (err) {
										ctx[key] = null;
									}
							});
						});
						cb( ctxs[0] );
					}
				});
			
			else
				cb( ctx );
			*/
		},
		
		runPlugin: function runPlugin(req, res) {  //< callback res(ctx || null) with results in ctx
		/**
		@method runPlugin
		Run a dataset-engine plugin named X = req.table using parameters Q = req.query
		or (if Q.id or Q.name specified) dataset X parameters derived from the matched  
		dataset (with json fields automatically parsed). On running the plugin's engine X, this 
		method then responds on res(results).   If Q.Save is present, the engine's results are
		also saved to the plugins dataset.  If Q.Job is present, then responds with res(Q.Job), 
		thus allowing the caller to place the request in its job queues.  Otherwise, if Q.Job 
		vacant, then responds with res(results).  If a Q.agent is present, then the plugin is 
		out-sourced to the requested agent, which is periodically polled for its results, then
		responds with res(results).  Related comments in FLEX.config.
		*/
			
			//Log("run req",req);
			FLEX.getContext( req.sql, req.group+"."+req.table, req.query, function (ctx) {
				
				//Log("get ctx", ctx);				
				if (ctx) {
					Copy(ctx,req.query);
					//Log("plugin req query", req.query);
					
					if ( ctx.Job )  // let host chipping-regulating service run this engine
						res( ctx );

					else
					if ( viaAgent = FLEX.viaAgent )  // allow out-sourcing to agents if installed
						viaAgent(req, res);

					else  // in-source the plugin and save returned results
						FLEX.runEngine(req, res);
				}
					
				else
					res( null );
			});

		},
		
		plugins: JSLAB.plugins,
		
		viaAgent: function( req, res ) {  //< out-source a plugin callback res(ctx || null)
		/**
		@method viaAgent
		Out-source the plugin to an agent = req.query.agent, if specified; otherwise, in-source the
		plugin. Callsback res(results, sql) with a possibly new sql thread.
		**/

			var
				fetch = FLEX.fetcher,
				sql = req.sql,
				query = req.query,
				jobname = "totem."+ req.client + "." + req.table + "." + (query.ID||0);

			//Log({viaagent: query});
			
			if (agent = query.agent)   // attempt out-source
				fetch(agent.tag( "?", Copy(query,{push:jobname})), function (jobid) {

					if ( jobid ) {
						Trace("FORKED AGENT FOR job-"+jobname,sql);

						sql.query("INSERT INTO queues SET ?", {
							class: agent,
							client: req.client,
							qos: 0,
							priority: 0,
							name: job.name
						});

						if ( poll = parseInt(query.poll) )
							var timer = setInterval(function (req) { 

								Trace("POLLING AGENT FOR job"+jobid);

								fetch(req.agent+"?pull="+jobid, function (ctx) {

									if (ctx)
										FLEX.thread( function (sql) {
											Trace("FREEING AGENT FOR job-"+jobid, sql);
											sql.query("DELETE FROM app.queues WHERE ?", {Name: plugin.name});								
											sql.release();
											req.cb( ctx );
										});
									
									else {
										Trace("FAULTING AGENT");
										clearInterval(timer);
										req.cb( null );
									}

								});

							}, poll*1e3, Copy({cb: res}, req));
					}

				});

			else  // in-source
				FLEX.runEngine(req, res);
		},
		
		config: function (opts) {
		/**
		@method config
		Configure module with spcified options.   Add the FLEX.runPlugin method to the
		FLEX.execute interface for every plugin in the FLEX.plugins.  These plugins are
		also published to app.engines (disabled and only if they dont already exist).  Engines 
		are scanned to prime their corresponding dataset (if they dont already exist).
		*/
			function engineType(eng) {
				if ( eng.constructor == String ) 
					if ( eng.indexOf("function") >= 0 )
						return "ma";
					else
					if ( eng.indexOf("def") >= 0 )
						return "py";
					else
						return "?";
				else
					return "js";
			}
			
			if (opts) Copy(opts,FLEX);
			
			if (FLEX.thread)
			FLEX.thread( function (sql) {
				
				ENUM.extend(sql.constructor, [
					selectJob,
					deleteJob,
					updateJob,
					insertJob,
					executeJob,
					/*
					selectData,
					deleteData,
					updateData,
					insertData,
					executeData,
					*/
					hawkCatalog,
					queryDS, 
					flattenCatalog,
					
					beginBulk,
					endBulk
					
					//hawkJobs
				]);

				//sql.hawkJobs("flex", FLEX.site.masterURL);

				READ.config(sql);			
				
				if (CLUSTER.isMaster)
					if (runPlugin = FLEX.runPlugin)  // add builtin plugins to FLEX.execute
						for (var name in FLEX.plugins) {
							FLEX.execute[name] = runPlugin;
							Trace("PUBLISHING "+name, sql);

							if ( name != "plugins")
								if ( plugin = FLEX.plugins[name] )
									sql.query( 
										"REPLACE INTO app.engines SET ?", {
											Name: name,
											Code: plugin + "",
											State: "{}",  //JSON.stringify({Port:name}),
											Type: engineType(plugin),
											Enabled: 1
										});
							}

				if (false)
					sql.query("SELECT Name FROM app.engines WHERE Enabled")
					.on("result", function (eng) { // prime the associated dataset
						Trace("PRIME plugin-"+eng.Name, sql);
						sql.query(
							"CREATE TABLE app.?? (ID float unique auto_increment, Name varchar(32), "
							+ "Description mediumtext)",
							eng.Name );
					});
				
				sql.release();
			});
			
			var
				site = FLEX.site,
				email = FLEX.mailer;
				
			if (CLUSTER.isMaster) {
				
				// setup news feeder
				
				NEWSFEED = new FEED({					// Establish news feeder
					title:          site.nick,
					description:    site.title,
					link:           `${FLEX.paths.HOST}/feed.view`,
					image:          'http://example.com/image.png',
					copyright:      'All rights reserved 2013',
					author: {
						name:       "tbd",
						email:      "noreply@garbage.com",
						link:       "tbd"
					}
				});

				/*
				if (FLEX.likeus.PING)  // setup likeus table hawk
					setInterval( function() {
						
						FLEX.thread(function (sql) {
							
							Trace("PROFILES UPDATED");
							
							sql.query(
								"SELECT *, datediff(now(),Updated) AS Age FROM openv.profiles WHERE LikeUs HAVING Age>=?",
								[FLEX.likeus.BILLING]
							)
							.on("result", function (rec) {		
								sql.query("UPDATE openv.profiles SET LikeUs=LikeUs-1 WHERE ?",{ID:rec.ID} );
							})
							.on("end",sql.release);
							
						});
						
					}, FLEX.likeus.PING*(3600*24*1000) );
					*/
				
				if (email) {
					email.TX = {};
					email.RX = {};
					
					var parts = (site.emailhost||"").split(":");
					email.TX.HOST = parts[0];
					email.TX.PORT = parseInt(parts[1]);

					var parts = (site.emailuser||"").split(":");
					email.USER = parts[0];
					email.PASS = parts[1];

					if (email.TX.PORT) {  		// Establish server's email transport
						Trace(`MAILHOST ${email.TX.HOST} ON port-${email.TX.PORT}`);

						email.TX.TRAN = email.USER
							? MAIL.createTransport({ //"SMTP",{
								host: email.TX.HOST,
								port: email.TX.PORT,
								auth: {
									user: email.USER,
									pass: email.PASS
								}
							})
							: MAIL.createTransport({ //"SMTP",{
								host: email.TX.HOST,
								port: email.TX.PORT
							});

					}
					else
						email.TX = {};

					if (email.RX.PORT)
						email.RX.TRAN = new IMAP({
							  user: email.USER,
							  password: email.PASS,
							  host: email.RX.HOST,
							  port: email.RX.PORT,
							  secure: true,
							  //debug: function (err) { console.warn(ME+">"+err); } ,
							  connTimeout: 10000
							});
					else
						email.RX = {};

					if (email.RX.TRAN)					// Establish server's email inbox			
						openIMAP(function(err, mailbox) {
						  if (err) killIMAP(err);
						  email.RX.TRAN.search([ 'UNSEEN', ['SINCE', 'May 20, 2012'] ], function(err, results) {
							if (err) killIMAP(err);
							email.RX.TRAN.fetch(results,
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
								email.RX.TRAN.logout();
							  }
							);
						  });
						});

					if (email.ONSTART) 
						sendMail({
							to: site.pocs.admin,
							subject: site.title + " started", 
							body: "Just FYI"
						});
				}
			}			
		}

};

/*
// Job queue CRUDE interface
FLEX.select.jobs = function Xselect(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	// route valid jobs matching sql-where clause to its assigned callback res(job).
	
	Trace( sql.query(
		"SELECT *, datediff(now(),Arrived) AS Age,openv.profiles.* FROM queues LEFT JOIN openv.profiles ON queues.Client=openv.profiles.Client WHERE NOT Hold AND Departed IS NULL AND NOT openv.profiles.Banned AND least(?,1) ORDER BY queues.QoS,queues.Priority", 
		guardQuery(query,true), function (err, recs) {
			res( err || recs );
	}));
	
}

FLEX.update.jobs = function Update(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
	
	// adjust priority of jobs matching sql-where clause and route to callback res(job) when updated.
		
	if (protect)
		res( FLEX.errors.protectedQueue );
	else
		sql.query("UPDATE queues SET ? WHERE ?",body,query, function (err,info) {
			res( err || info );
		});
}
		
FLEX.delete.jobs = function Delete(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
		
	if (protect)
		res( FLEX.errors.protectedQueue );
	else
		sql.query("DELETE FROM queues WHERE ?",query, function (err,info) {
			res( err || info );
		});
}

FLEX.insert.jobs = function Insert(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
		
	if (protect)
		res( FLEX.errors.protectedQueue );
	else
		sql.query("INSERT INTO queues SET ?",body, function (err,info) {
			res( err || info );
		});
}
*/

/**
@class SQL engines interface
*/

/*
FLEX.select.sql = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		sql.query(
			"SELECT SQL_CALC_FOUND_ROWS * FROM ?? "+(rec.Code||"WHERE least(?,1)").format(query),
			["app."+log.Table,query],
			function (err, recs) {

				res(err || recs);
		});
	});
}

FLEX.insert.sql = function Insert(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"INSERT INTO ?? "+(rec.Code||"SET ?"),
				["app."+rec.Name, req.body],
				function (err, recs) {
					
			res(err || recs);
		});
	});
}

FLEX.delete.sql = function Delete(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"DELETE FROM ?? "+(rec.Code||"WHERE least(?,1)").format(query),
				["app."+log.Table,query],
				function (err, recs) {	
					
			res(err || recs);
		});
	});
}

FLEX.update.sql = function Update(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT Code,Name FROM engines WHERE LEAST(?) LIMIT 0,1",{Enabled:true,Engine:"select",Name:log.Table})
	.on("result", function (rec) {
		
		sql.query(
				"UPDATE ?? "+(rec.Code||"SET ? WHERE least(?,1)").format(query),
				["app."+rec.Name, req.body,query],
				function (err, recs) {	
					
			res(err || recs);
		});
	});
}
*/

/**
@class GIT interface
*/

FLEX.execute.baseline = function Xexecute(req,res) {  // baseline changes

	var	sql = req.sql, 
		query = req.query,
		user = `-u${ENV.MYSQL_USER} -p${ENV.MYSQL_PASS}`,
		ex = {
			group: `mysqldump ${user} ${req.group} >admins/db/${req.group}.sql`,
			openv: `mysqldump ${user} openv >admins/db/openv.sql`,
			clear: `mysql ${user} -e "drop database baseline"`,
			prime: `mysql ${user} -e "create database baseline"`,
			rebase: `mysql ${user} baseline<admins/db/openv.sql`,
			commit: `git commit -am "${req.client} baseline"`
		};
	
	sql.query("SELECT sum(Updates) AS Changes FROM openv.journal", function (err,recs) {
		if (err)
			res( FLEX.errors.badbaseline );
		
		else
			res( 
				(query.noarchive ? "Bypass database commit" : "Database commited. ") +
				(recs[0].Changes || 0)+" changes since last baseline.  Service restarted.  " +
				"Return "+"here".link("/home.view")
			);

		sql.query("DELETE FROM openv.journal");
	});
	
	if (!query.noarchive)
	CP.exec(ex.group, function (err,log) {
	Trace("BASELINE GROUP "+(err||"OK"), sql);

	CP.exec(ex.openv, function (err,log) {
	Trace("BASELINE OPENV "+(err||"OK"), sql);

	CP.exec(ex.clear, function (err,log) {
	Trace("BASELINE CLEAR "+(err||"OK"), sql);

	CP.exec(ex.prime, function (err,log) {
	Trace("BASELINE PRIME "+(err||"OK"), sql);

	CP.exec(ex.rebase, function (err,log) {
	Trace("BASELINE REBASE "+(err||"OK"), sql);

	CP.exec(ex.commit, function (err,log) {
	Trace("BASELINE COMMIT "+(err||"OK"), sql);

		if (!query.noexit) process.exit();				
		
	});
	});
	});
	});
	});
	});
	
}

FLEX.select.git = function Xselect(req, res) {

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

/**
@class EMAIL peer-to-peer email exchange interface
*/
	
FLEX.select.email = function Xselect(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	sql.query("SELECT * FROM app.emails WHERE Pending", function (err,recs) {
		res(err || recs);

		sql.query("UPDATE emails SET Pending=0 WHERE Pending");
	});
	
}
	
FLEX.execute.email = function Xexecute(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	res(SUBMITTED);
	
	if (false)
	requestService(srv, function (rec) {
		sendMail({
			to:  rec.To,
			subject: rec.Subject,
			body: rec.Body
		}, sql);
	});
	
}

// Master catalog interface

/*FLEX.select.CATALOG = function Xselect(req, res) {
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
*/

FLEX.execute.catalog = function Xexecute(req, res) {
	var 
		sql = req.sql, log = req.log, query = req.query, flags = req.flags,
		catalog = FLEX.flatten.catalog || {},
		limits = FLEX.flatten.limits || {};
	
	limits.pivots = flags.pivot || "";
	limits.stamp = new Date();

	if (false)
		Log({
			cat: catalog,
			lim: limits
		});
	
	res(SUBMITTED);
	
	sql.query("DELETE from catalog");
	
	sql.flattenCatalog( flags, catalog, limits, function (recs) {
		
		Trace("FLATTEN CATALOG RECS "+recs.length, sql);
		
		recs.each( function (n,rec) {
			sql.query("INSERT INTO catalog SET ?",rec);
		});

	});
	
}

FLEX.select.plugins = function Xselect(req,res) {
	var sql = req.sql, query = req.query;
	var plugins = [];
	
	sql.query("SELECT Name FROM ??.engines WHERE Enabled", req.group, function (err,engs) {		
		
		if ( err )
			res( plugins );
		
		else
			engs.each(function (n,eng) {

				sql.query("SHOW TABLES FROM ?? WHERE ?", [
					req.group, {tables_in_app: eng.Name}
				], function (err,recs) {

					if ( !err && recs.length) 
						plugins.push( {
							ID: plugins.length,
							Name: eng.Name.tag("a",{href:"/"+eng.Name+".run"}) 
						});

					if (n == engs.length-1) res( plugins );
				});
			});
	});
	
}

FLEX.select.tasks = function Xselect(req,res) {
	var sql = req.sql, query = req.query;
	var tasks = [];
	
	sql.query("SELECT Task FROM openv.milestones GROUP BY Task", function (err,miles) {
		
		if ( !err )
			miles.each(function (n,mile) {
				tasks.push({
					ID: tasks.length,
					Name: mile.Task.tag("a",{href: "/task.view?options="+mile.Task})
				});
			});
		
		res( tasks );		
		
	});
}

/*
FLEX.delete.files = function Xselect(req,res) {
	var sql = req.sql, query = req.query;

	res( SUBMITTED );
	
	sql.each("DELETEFILE", "SELECT * FROM app.files WHERE least(?,1)", {
		ID:query.ID,
		Client: req.client
	}, function (file) {
		
		var 
			area = file.Area,
			name = file.Name,
			path = `${area}/${name}`,
			pub = "./public",
			arch = `${pub}/${path}`,
			zip = `${arch}.zip`;

		CP.exec(`zip ${zip} ${arch}; git commit -am "archive ${path}"; git push github master; rm ${zip}`, function (err) {
		CP.exec(`rm ${arch}`, function (err) {
			//sql.query("DELETE FROM app.files WHERE ?",{ID:query.ID});

			sql.query("UPDATE app.files SET State='archived' WHERE least(?)", {Area:file.Area, Name:file.Name});

			sql.query( // credit the client
				"UPDATE openv.profiles SET useDisk=useDisk-? WHERE ?", [ 
					file.Size, {Client: req.client} 
			]);
		});
		});
	});
	
}

FLEX.execute.files = function Xselect(req,res) {
	var sql = req.sql, query = req.query;

	res( SUBMITTED );
	
	sql.each("EXEFILE", "SELECT * FROM app.files WHERE least(?,1)", {
		ID:query.ID,
		Client: req.client
	}, function (file) {
		
		var 
			area = file.Area,
			name = file.Name,
			path = `${area}/${name}`,
			pub = "./public",
			arch = `${pub}/${path}`,
			zip = `*.zip`;

		CP.exec(`git commit -am "archive ${path}"; git push github master; rm ${zip}`, function (err) {
		});
	});
	
}
*/

// Getters

FLEX.select.activity = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var recs = {};
	
	sql.query(
		"SELECT * FROM openv.attrs", 
		[] , 
		function (err,attrs) {
			
	sql.query(
		'SELECT `Table`,sum(action="Insert") AS INSERTS,'
	+ 'sum(action="Updates") as UPDATES,'
	+ 'sum(action="Select") as SELECTS,'
	+ 'sum(action="Delete") as DELETES,'
	+ 'sum(action="Execute") as EXECUTES  FROM dblogs '
	+ 'WHERE datediff(now(),Event)<30 GROUP BY `Table`',
		[],
		function (err,acts) {
			
			attrs.each( function (n,dsattr) {
				var rec = recs[dsattr.Table];
				
				if (!rec) rec = {
					INSERT: 0,
					SELECTS: 0,
					DELETES: 0,
					UPDATES: 0,
					EXECUTES: 0
				};
				
				Copy( dsattr, rec );
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
			
			res( FLEX.listify(recs, "ID") );
	});
	});
}

FLEX.select.views = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var views = [];
	var path = `./public/jade/`;
	
	FLEX.indexer( path , function (files) {
		
		files.each(function (n,file) {
		
			var stats = FS.statSync(path + file);		
			
			if (stats.isDirectory())
				views.push( { ID: n, Name: file, class: `${file}/` } );
				
		});
	});
	
	res(views);
}

FLEX.select.links = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var 
		path = "./public/jade/" + unescape(query.area || "") + "/",
		id = 0,
		taken = {},
		links = [{
			ID: id++,
			Name: "&diams; HOME".tag("a",{
				href: "home.view"
			})
		}];

	FLEX.indexer( path, function (files) {

		files.each(function (n,file) {
		
			var stats = FS.statSync(path + file);
			
			if (stats.isDirectory()) 
				if (file.charAt(0) == ".") 
					FLEX.indexer( path+file, function (subs) {
						subs.each(function (n,sub) {
							links.push({
								ID: id++,
								Name: (file + " &rrarr; "+sub).tag("a",{
									href: `${name}.view`
								})
							});
							taken[sub] = 1;
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
				var name = file.replace(".view","");
				
				if ( !taken[name] )
					links.push({
						ID: id++,
						Name: name.tag("a", {
							href: `${name}.view`
							/*(FLEX.statefulViews[name] ? FLEX.WORKER : "") + area + file*/
						})
					});
			}
			
		});
	});
	
	res(links);
}

/*
FLEX.select.themes = function (req, res) {
	var themes = [];
	var path = ENV.THEMES;

	FLEX.indexer( path , function (files) {
		files.each( function (n,file) {
			//var stats = FS.statSync(path + "/"+file);

			if ( file.charAt(file.length-1) != "/" ) //(!stats.isDirectory())
				themes.push( {ID: n, Name: file, Path: file} );
		});
	});
	
	res(themes);	
}
*/

FLEX.select.summary = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var cnts = FLEX.diag.counts;
	
	// Could check rtn.serverStatus and warningCount (become defined when a MySQL table corrupted).  
	// Return selected record info (fieldCount, affectedRows, insertId, serverStatus, warningCount, msg)
				
	res(cnts.State
		? [ {ID:0, Name: cnts.State},
			{ID:1, Name: JSON.stringify(cnts).replace(/"/g,"").tag("font",{color: (cnts.State=="ok") ? "green" : "red"})}
		  ]
		: [ {ID:0, Name: "pending"} ]
	);
}

FLEX.select.users = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,Connects,Client AS Name, Location AS Path FROM openv.sessions WHERE least(?,1) ORDER BY Client", 
		guardQuery(query,true),
		function (err, recs) {
			res(err || recs);
	});
}

/*
FLEX.select.ENGINES = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,engineinfo(Name,Type,Updated,Classif,length(Code),Period,Enabled,length(Special)) AS Name FROM engines WHERE least(?,1) ORDER BY Name",
		guardQuery(query,true), 
		function (err, recs) {
			res(err || recs);
	});
}
*/

FLEX.select.config = function Xselect(req, res) {
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
			classif: FLEX.CLASSIF,
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
			//routes: JSON.stringify(FLEX.routes),
			//netif: JSON.stringify(OS.networkInterfaces()), 	//	do not provide in secure mode
			//temp: TEMPIF.value()  // unavail on vms
		});
	});
	});
	});
}
	
FLEX.select.datasets = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var rtns = [], ID=0;
	
	sql.eachTable( "app", function (table) {
		rtns.push({
			Name: table.tag("a",{href:"/"+table+".db"}),
			ID: ID++
		});
	});

	for (var n in FLEX.select)
		rtns.push({
			Name: n.tag("a",{href:"/"+n+".db"}),
			ID: ID++
		});

	res(rtns);
	
	/*sql.query("SHOW TABLES")
	.on("result", function (rec) {
		rtns.push({
			Name: rec.Tables_in_app1.tag("a",{href:"/"+rec.Tables_in_app1+".db"}),
			ID: ID++
		});
	})
	.on("end", function () {
		for (var n in FLEX.select)
			rtns.push({
				Name: n.tag("a",{href:"/"+n+".db"}),
				ID: ID++
			});
			
		res(rtns);
	}); */
}

FLEX.select.admin = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT *,avg_row_length*table_rows AS Used FROM information_schema.tables", [], function (err, recs) {
		res(err || recs);
	});
}
	
/*
FLEX.select.queues = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,queueinfo(Job,min(Arrived),timediff(now(),min(Arrived))/3600,Client,Class,max(State)) AS Name FROM app.queues GROUP BY Client,Class", 
		[],
		function (err, recs) {
			res(err || recs);
	});
}
*/

FLEX.select.cliques = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	 
	res([]);
	/*
	sql.query(
		  "SELECT dblogs.ID AS ID,cliqueinfo(dblogs.ID,count(DISTINCT user),concat(ifnull(dsattr,'none'),'-',ifnull(detectors.name,'nill')),group_concat(DISTINCT user,';')) AS Name "
		  + "FROM dblogs LEFT JOIN detectors ON (detectors.id=dblogs.recid AND dblogs.table='detectors') WHERE recid AND least(?,1) GROUP BY dsattr,detectors.name",
		
		guardQuery(query,true),  function (err, recs) {
			res(err || recs);
	});*/
}

FLEX.select.health = function Xselect(req, res) {
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
//Log(dfout);

	CP.exec("netstat -tn", function (err,nsout,nserr) {
//Log(nsout);

	sql.query(
		"SELECT "
		+ "round(avg(Transfer/Delay)*1e-3,2) AS avg_KBPS, "
		+ "round(max(Transfer/Delay)*1e-3,2) AS max_KBPS, "
		+ "sum(Fault !='') AS faults, "
		+ "round(sum(Transfer)*1e-9,2) AS tot_GB, "
		+ "count(ID) AS logs "
		+ "FROM app.dblogs")
	.on("error", function (err) {
		Log(err);
	})
	.on("result", function (lstats) {

//Log(lstats);

	sql.query(
		  "SELECT "
		+ "sum(departed IS null) AS backlog, "
		+ "avg(datediff(ifnull(departed,now()),arrived)) AS avg_wait_DAYS, "
		+ "sum(Age)*? AS cost_$ "
		+ "FROM app.queues",[4700])
	.on("error", function (err) {
		Log(err);
	})
	.on("result", function (qstats) {
	
//Log(qstats);

	sql.query("SHOW GLOBAL STATUS", function (err,dstats) {
		
//Log(dstats);

	sql.query("SHOW VARIABLES LIKE 'version'")
	.on("result", function (vstats) {
	
		dstats.each( function (n,stat) { 
			if ( stat.Variable_name in dbstats )
				dbstats[stat.Variable_name] = stat.Value; 
		});
		
//Log(dbstats);

		Each(qstats, function (n,stat) {
			rtns.push({ID:ID++, Name:"job "+n.tag("a",{href:"/admin.view?goto=Jobs"})+" "+stat});
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
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.view?goto=Logs"})+" "+stat});
		});

		Each(lstats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.view?goto=Logs"})+" "+stat});
		});

		var stats = {
			rw_GB: 
				(dbstats.Innodb_data_read*1e-9).toFixed(2) + "/" +
				(dbstats.Innodb_data_written*1e-9).toFixed(2),
				
			tx_K: (dbstats.Queries*1e-3).toFixed(2),
	
			up_DAYS: (dbstats.Uptime/3600/24).toFixed(2)			
		};
			
		Each(stats, function (n,stat) {
			rtns.push({ID:ID++, Name:isp+" "+n.tag("a",{href:"/admin.view?goto=DB Config"})+" "+stat});
		});

		var nouts = nsout ? nsout.split("\n") : ["",""],
			naddr = nouts[1].indexOf("Local Address");

		var stats = {
			classif: FLEX.CLASSIF,
			cpu: OS.cpus()[0].model,
			host: OS.hostname() 			// do not provide in secure mode
			//cpus: JSON.stringify(OS.cpus()),
			//routes: JSON.stringify(FLEX.routes),
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
			rtns.push({ID:ID++, Name:asp+" "+n.tag("a",{href:"/admin.view?goto=SWAPs"})+" "+stat});
		});

		res(rtns);
	});
	/*
	.on("end", function () {
		
		var maxage = age = vol = files = 0;
		
		for (var area in {UPLOADS:1, STORES:1, SHARES:1} )
			FLEX.indexer( ENV[area], function (n,file) {
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

FLEX.select.likeus = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	//TRACE(FLEX.site.pocs);
	
	if ( FLEX.site.pocs.admin )
		sendMail({
			to:  FLEX.site.pocs.admin,
			subject: req.client + " likes " + FLEX.site.title + " !!",
			body: "Just saying"
		}, sql);

	var user = {
		expired: "your subscription has expired",
		0: "elite",
		1: "grand",
		2: "wonderful",
		3: "better than average",
		4: "below average",
		5: "first class",
		default: "limtied"
	}			
	
	if ( req.profile.Credit ) {
		sql.query(
			"UPDATE openv.profiles SET Challenge=0,Likeus=Likeus+1,QoS=greatest(QoS-1,0) WHERE ?",
			{Client:req.client}
		);

		req.profile.QoS = Math.max(req.profile.QoS-1,0);  // takeoff 1 sec
		var qos = user[Math.floor(req.profile.QoS)] || user.default;
		res( `Thanks ${req.client} for liking ${FLEX.site.nick} !  As a ${qos} user your ` 
			+ "QoS profile".tag("a",{href:'/profile.view'})
			+ " may have improved !" )	
	}
	
	else
		res( `Thanks ${req.client} for liking ${FLEX.site.nick} but ` +
			user.expired.tag("a",{href:'/fundme.view'}) + " !" );
		
}

FLEX.select.tips = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	var q = sql.query(
		"SELECT *, "
		+ "count(detects.ID) AS weight, "
		+ "concat('/tips/',chips.ID,'.jpg') AS tip, "
		+ "concat("
		+ 		"linkquery('O', 'https://ldomar.ilabs.ic.gov/omar/mapView/index',concat('layers=',collects.layer)), "
		+ 		"linkquery('S', '/minielt.view',concat('src=/chips/',chips.name))"
		+ ") AS links, "
		+ "datediff(now(),collected) AS age "
		+ "FROM app.detects "
		+ "LEFT JOIN app.chips ON MBRcontains(chips.address,detects.address) "
		+ "LEFT JOIN app.collects ON collects.ID = chips.collectID "
		+ "LEFT JOIN app.detectors ON detectors.name = detects.label "
		+ "WHERE least(?,1) "
		+ "GROUP BY detects.address "
		+ "ORDER BY detects.made DESC "
		+ "LIMIT 0,400", 
		guardQuery(query,true), 
		function (err, recs) {

			if (err)
				Log("tips",[err,q.sql]);
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

FLEX.select.history = function Xselect(req,res) {
	var sql = req.sql, log = req.log, query = req.query;

	var pivots = {
		"both": "journal.Dataset,journal.Field",
		bydataset: "journal.Dataset",
		byfield: "journal.Field"
	},
		pivot = pivots[query.pivot || "bydataset"] || pivots.both;
	
	sql.query("USE openv", function () {
		
		switch (query.options) {

			case "signoffs":
				//var comment = "".tag("input",{type:"file",value:"mycomments.pdf",id:"uploadFile", multiple:"multiple",onchange: "BASE.uploadFile()"});
				
				sql.query(
					"SELECT "
					+ "Hawk, max(power) AS Power, "
					//+ "? AS Comment, "
					//+ 'approved' AS Comment, "
					//+ "link(concat('Files|Upload|', Hawk, '.pdf'), concat('/uploads/', Hawk, '.pdf')) AS DetailedComments, "
					+ "group_concat(distinct ifnull(link(journal.dataset,concat('/',viewers.viewer,'.view')),journal.dataset)) AS Changes,"
					+ "link('Approve', concat('/history.db', char(63), 'options=approve'," 
					+ 		" '&Power=', Power,"
					+ 		" '&Hawk=', Hawk,"
					+ 		" '&Datasets=', group_concat(distinct journal.dataset))) AS Approve,"
					+ "group_concat(distinct journal.field) AS Fields,"
					+ "linkrole(hawk,max(updates),max(power)) AS Moderator,"
					+ "false as Approved "
					+ "FROM openv.journal "
					+ "LEFT JOIN viewers ON journal.Dataset=viewers.Dataset "
					+ "WHERE Updates "
					+ `GROUP BY hawk,${pivot}`,
					function (err, recs) {
						
						res( err || recs );
					});
				break;
					
			case "changes":

				sql.query(
				"SELECT "
				+ "group_concat(distinct concat(Dataset,'.',Field)) AS Datasets,"
				+ "group_concat(distinct linkrole(Hawk,Updates,Power)) AS Moderators "
				+ "FROM openv.journal "
				+ "WHERE Updates "
				+ `GROUP BY ${pivot}`, 
					function (err, recs) {
					
					res( err || recs );
				});
				break;
				
			case "earnings":
				sql.query(
					"SELECT Client, group_concat(distinct year(Reviewed),'/',month(Reviewed), ' ', Comment) AS Comments, "
					+ "group_concat( distinct linkrole(roles.Hawk,roles.Earnings,roles.Strength)) AS Earnings "
					+ "FROM openv.roles "
					+ "LEFT JOIN journal ON roles.Hawk=journal.Hawk "
					+ "WHERE Updates "
					+ "GROUP BY Client, roles.Hawk", 
					function (err, recs) {
					
					res( err || recs );
				});
				break;
				
			case "approve":
				res( "Thank you for your review -" 
						+ " revise your comments".hyper(`/roles.view?client=${req.client}`)
						+ " as needed" );
		
				sql.query(
					"SELECT sum(Updates)*? AS Earnings FROM openv.journal WHERE ?", [
					query.Power, {Hawk:query.Hawk}], function (err,earns) {
						var 
							earn = earns[0] || {Earnings: 0},
							log = {
								Client: req.client,
								Hawk: query.Hawk,
								Reviews: 1,
								Datasets: query.Datasets,
								Earnings: earn.Earnings,
								Comment: "Approved",
								Power: query.Power,
								Reviewed: new Date()
							};
						
						sql.query(
							"INSERT INTO roles SET ? ON DUPLICATE KEY UPDATE "
							+ "Reviews=Reviews+1, Earnings=Earnings+?, Comment=?, Reviewed=?, Datasets=?", [
								log, log.Earnings, log.Comment, log.Reviewed, log.Datasets] );
						
						if (query.Datasets) // reset journalled updates
						query.Datasets.split(",").each( function (n,dataset) {
							sql.query(
								"UPDATE openv.journal SET Updates=0 WHERE least(?)", {
									Hawk: query.Hawk,
									Dataset: dataset
							});
						});

						if (query.Power) // notify greater and lesser hawks of approval
						sql.query("SELECT Client FROM openv.roles WHERE Power>=?", query.Power, function (err,ghawk) { // greater hawks
						sql.query("SELECT Client FROM openv.roles WHERE Power<?", query.Power, function (err,lhawk) { // lesser hawks

							var to=[], cc=[];

							ghawk.each(function (n,hawk) {
								to.push( hawk.client );
							});
							lhawk.each(function (n,hawk) {
								cc.push( hawk.client );
							});

							if (to.length)
								sendMail({
									to: to.join(";"),
									cc: cc.join(";"),
									subject: "review completed",
									body: "for more information see " 
										+ "moderator comments".hyper("/moderate.view") 
										+ " and " 
										+ "project status".hyper("/project.view")
								}, sql);
						});
						});
				});
				break;
				
			case "itemized":
			default:
					
				sql.query(
					"SELECT concat(Dataset,'.',Field) AS Idx, "
					+ "group_concat(distinct linkrole(Hawk,Updates,Power)) AS Moderators "
					+ "FROM openv.journal "
					+ "WHERE Updates "
					+ `GROUP BY ${pivot}`, 
					function (err,recs) {

					res( err || recs );
				});

		}
	});

}

// Digital globe interface

FLEX.select.DigitalGlobe = function Xselect(req, res) {  
	var sql = req.sql, log = req.log, query = req.query;
}

// Hydra interface

FLEX.select.AlgorithmService = function Xselect(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	var args =  {		// Hydra parameters
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
	sql.query("SELECT * FROM app.detectors WHERE ?", {Channel:query.CHANNEL})
	.on("result", function (det) {
		
		if (FLEX.CHIP)
		FLEX.CHIP.workflow(sql, {
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
				link: det.Name.tag("a",{href:"/swag.view?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
		
	});

}

// Reserved for NCL and ESS service alerts
FLEX.select.NCL = function Xselect(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
}

FLEX.select.ESS = function Xselect(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
}

// Uploads/Stores file interface

FLEX.select.uploads = FLEX.select.stores = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query,  body = req.body;
	var now = new Date();			
	var rtns = [], area = req.table;
	var path = `./public/${area}`;

	Trace(`INDEX ${path} FOR ${req.client}`, sql);
		  
	switch (area) {
		
		case "uploads":
		case "stores":
		case "proofs":
		case "shares":
		
			FLEX.indexer( path, function (files) {
				
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
										Name	: 
											file.tag("a", {
												href:"/files.view?goto=ELT&options="+`${area}.${file}`
											}) 	+ 
												"".tag("img",{src:link, width: 32, height: 32}),
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
				FLEX.indexer( `./public/${area}`, function (n,file) {

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
				"SELECT Area,Name,concat(Area,'.',Name) AS Ref, link(Name,concat('/',Area,'.',Name)) AS Link, astext(address) AS Address FROM app.files HAVING ?", 
				guardQuery(query,true), function (err,rtns) {
					res(err || rtns);
			});
	}
}

FLEX.update.stores = FLEX.insert.stores =
FLEX.update.uploads = FLEX.insert.uploads = function Xupdate(req, res) {
	
	var 
		sql = req.sql, 
		query = req.query, 
		body = req.body,
		canvas = body.canvas || {objects:[]},
		attach = [],
		now = new Date(),
		image = body.image,
		area = req.table,
		geoloc = query.location || "POINT(0 0)",
		files = image ? [{
			name: name, // + ( files.length ? "_"+files.length : ""), 
			size: image.length/8, 
			image: image
		}] : body.files || [];

	/*
	Log({
		q: query,
		b: body,
		f: files,
		a: area,
		l: geoloc
	});*/
					
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

	if (uploader = FLEX.uploader)
	uploader(files, area, function (file) {

		Trace(`UPLOAD ${file.filename} INTO ${area} FOR ${req.group}.${req.client}`, sql);
		
		sql.query(	// this might be generating an extra geo=null record for some reason.  works thereafter.
			   "INSERT INTO ??.files SET ?,Location=GeomFromText(?) "
			+ "ON DUPLICATE KEY UPDATE Client=?,Added=now(),Revs=Revs+1,Location=GeomFromText(?)", [ 
				req.group, {
						Client: req.client,
						Name: file.filename,
						Area: area,
						Added: new Date(),
						Classif: query.classif || "",
						Revs: 1,
						Size: file.size,
						Tag: query.tag || ""
					}, geoloc, req.client, geoloc
				]);

		if (false)
		sql.query( // credit the client
			"UPDATE openv.profiles SET Credit=Credit+?,useDisk=useDisk+? WHERE ?", [ 
				1000, file.size, {Client: req.client} 
			]);
		
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
						"SELECT detectors.ID, count(ID) AS counts FROM app.detectors LEFT JOIN proofs ON proofs.label LIKE detectors.PosCases AND proofs.name=? HAVING counts",
						[area+"."+name]
					)
					.on("result", function (det) {
						sql.query("UPDATE detectors SET Dirty=Dirty+1");
					});

					break;
					
			}
	});

}

FLEX.execute.uploads = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	
	sql.query("DELETE FROM app.files WHERE Area LIKE 'geoNode.%'");
	
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

// Execute interfaces

/*
FLEX.execute.intake = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	sql.query("SELECT intake.ID,count(dblogs.ID) AS Activity,max(dblogs.Event) AS LastTx FROM intake LEFT JOIN dblogs ON dblogs.RecID=intake.ID  group by intake.ID HAVING LEAST(?,1)",query)
	.on("result", function (log) {
//Log(log);
	
	sql.query("SELECT Name,Special FROM intake WHERE ? AND Special IS NOT NULL AND Special NOT LIKE '//%'",[{ID:log.ID},{Special:null}])
	.on("result",function (sys) {
//Log(sys);

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
//Log([cnts,maxpdf]);

	sql.query("SELECT count(ID) AS Count FROM queues WHERE LEAST(?)",{State:0,Job:sys.Name,Class:"tips"})
	.on("result", function (users) {

	if (false)	// update if unset
		sql.query("SELECT "+stat.joinify()+" FROM intake WHERE ?",{ID:sys.ID})
		.on("result",function (sysstat) {
			
		for (var n in stat) if (!sysstat[n]) sysstat[n] = stat[n];
			
		sql.query("UPDATE intake SET ? WHERE ?",[sysstat,{ID: sys.ID}], function () {
//Log("updated");	
		}); // update
		}); // stats
	else 		// unconditionally update
		sql.query("UPDATE intake SET ? WHERE ?",[stat,{ID: sys.ID}], function () {
//Log("updated");	
		}); // update
	
	}); // users
	}); // clients
	}); // sys
	}); // log
	
	res(SUBMITTED);
}					
*/

/*
FLEX.update.engines = function Update(req, res) {
	var sql = req.sql, log = req.log, query = req.query, body = req.body;

	sql.query("UPDATE engines SET ? WHERE ?", [body,query])
	.on("end", function () {
		ENGINE.free(req,res);
	});
}*/

/*
FLEX.execute.engines = function Xexecute(req, res) {
	var sql = req.sql, query = req.query, body = req.body;
	var 
		Engine = query.Engine, 
		Name = query.Name, 
		Path = `./public/${Engine}/${Name}.${Engine}`;
	
	function compileEngine(engine, name, code, res) {
		try {
			VM.runInContext( "FLEX."+engine+"."+name+"="+code, VM.createContext({FLEX:FLEX})  );

			if (res) res("ok");
		}
		catch (err) {
			if (res) res(new Error(err+""));
		}
	}			
		
	Trace(`ACCESSING ${Name}.${Engine}`);
	
	if (Engine && Name)
		sql.query(
			"SELECT *,count(ID) AS Count FROM engines WHERE LEAST(?) LIMIT 0,1",
			{Engine:Engine,Name:Name}
		)
		.on("error", function (err) {
			res(err);
		})
		.on("result", function (eng) {

			if (eng.Count)  				// execute existing engine
				switch (eng.Engine) {
					case "select":			// update CRUDE interface
					case "delete":
					case "update":
					case "insert":
					case "execute":
						
						if (eng.Enabled)
							res( FLEX.errors.disableEngine );
						
						else 
							compileEngine(Engine,Name,eng.Code,res);
						
						break;
						
					case "jade":			// update jade skinner
						
						if (eng.Enabled)
							res( FLEX.errors.disableEngine );

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
								res( FLEX.errors.noEngine );
							}
								
						break;
						
					default:				// run simulation engine against located engine
						req.query = (eng.Vars.query||"").parse({});
						req.table = eng.Name;
						req.type = eng.Engine;
						ENGINE.select(req, res);
				}
			
			else 							// prime new engine
				switch (Engine) {
					case "select":		// create CRUDE interface
					case "delete":
					case "update":
					case "insert":
					case "execute":

						sql.query("INSERT into engines SET ?", {
							Code: (FLEX[Engine][Name]+""),
							Enabled: 0,
							Name: Name,
							Updated: new Date(),
							Engine: Engine
						}, function (err) {
							res(err || "primed");
						});
							
						break;

					default:  // prime from flex or from engine db
					
						if ( plugin = FLEX.execute[Name] ) 
							sql.query("INSERT into engines SET ?", {
								Code: plugin+"",
								Enabled: 0,
								Name: Name,
								Engine: "js"
							}, function (err) {
								res(err || "primed");
							});
							
						else
							FS.readFile(Path, 'utf-8', function (err,buf) {

								if (err) 
									res(err);

								else
									sql.query("INSERT into engines SET ?", {
										Code: buf,
										Enabled: 0,
										Name: Name,
										Engine: Engine
									}, function (err) {
										res(err || "primed");
									});
							});
				}
			
		});
	
	else
		res( FLEX.errors.missingEngine );
}
*/

FLEX.execute.milestones = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var map = {SeqNum:1,Num:1,Hours:1,Complete:1,Task:1};

	for (var n=0;n<=10;n++) map["W"+n] = 1;

	sql.query("DELETE FROM openv.milestones");
	FLEX.RDR.xlsx(sql,"milestones.xlsx","stores",function (rec) {
		for (var n in map) map[n] = rec[n] || "";
		
		sql.query("INSERT INTO openv.milestones SET ?",map, function (err) {
			if (err) Log(err);
		});
	});

	res(SUBMITTED);
}

FLEX.execute.sessions = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	if (FLEX.emitter)
		sql.query("SELECT * FROM openv.sessions WHERE length(Message)")
		.on("result", function (sock) {
			FLEX.emitter("alert", {msg: sock.Message, to: sock.Client, from: req.client});
			sql.query("UPDATE openv.sessions SET ? WHERE ?",[{Message:""},{ID:sock.ID}]);
		});
		
	res(SUBMITTED);
}

/*
FLEX.execute.tests = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	res(SUBMITTED);
	
	sql.query(
		"SELECT *,tests.ID AS testID,tests.Channel as chanName,detectors.Name as detName FROM tests "
		+ " LEFT JOIN detectors ON detectors.Channel=tests.Channel AND Enabled HAVING ?",
		{testID:query.ID}
	)
	.on("result", function (test) {
		
Log(req.profile);
		
		if (FLEX.CHIP)
		FLEX.CHIP.workflow(sql, {
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
				link: test.detName.tag("a",{href:"/swag.view?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
	});

}
*/

/*
FLEX.execute.collects = function Xexecute(req, res) {
	var req = req, sql = req.sql, log = req.log, query = req.query;
	
	/ *
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
								Log("apperr="+err);
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
}
*/

/*
FLEX.execute.detectors = function Xexecute(req, res) {
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
			
//Log([f,FPR,TPR,qual,TPR/FPR]);

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
	
	Trace("PRIME DETECTORS");
	
	if ( guardQuery(query,false) ) 	// clear previously locked proofs and retrain selected detectors
		sql.query("UPDATE proofs SET dirty=0,posLock=null,negLock=null WHERE posLock OR negLock", function () {
			sql.query("SELECT * FROM detectors WHERE least(?,1)",query)
			.on("result", function (det) {
				sql.jobs().execute(req.client, det);
			});
		});
	else {	// rescore detectors
		
		/ *
		sql.query("SELECT * FROM detectors WHERE Dirty")
		.on("result", function (det) {
			sql.jobs().execute(req.client, det);
			
			sql.query("UPDATE detectors SET Dirty=0 WHERE ?",{ID: det.ID});
		});
		* /

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
	
	/ *
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
	* /
	
}
*/

FLEX.execute.events = function Xexecute(req, res) {
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
				Log(['a'+event.job, Math.round(event.tArrive,4),  Math.round(terr,4), queue.length]);

				if ( ++n < N ) 			// more arrivals to scedule ?
					arrival();
					
				else {						// no more arrivals (there will be straggeling departures)
						if (summary)
						sql.query("select avg(tDelay) as avgDelay, avg(depth) as avgDepth, avg(tStep) as avgStep, avg(tService) as avgService FROM app.jobs")
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
										
								Log(JSON.stringify(rtns));
								break;
								
							case "rel":
								for (var idx in check) check[idx] = Math.round( (1 - check[idx].expected / check[idx].achived)*100, 4) + "%";
								Log(JSON.stringify(check));
								break;							
						}
					});

						if (bins) 			// record delay stats or levels
						sql.query("SELECT * FROM app.jobs WHERE ?", {label:label}, function (err, recs) {

							statJobs(recs, bins, function (stats) {
								
								sql.query("DELETE FROM app.jobstats WHERE ?", {label:label});
								
								if ( levels )  		// save delay times at prescibed levels of confidence
									levels.each( function (n, level) {
										Each( stats, function (bin, x) {
											if ( (1 - x.pr) < level ) {
												sql.query("INSERT INTO app.jobstats SET ?", {
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

		if (trace) Log(JSON.stringify(head));

		sched(head.tService, head, function (event, terr) {		// service departure
			
			Terr += terr;

			if (trace)
			Log(['d'+event.job, Math.round(event.tDepart, 4), Math.round(terr,4), queue.length]);
			
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
		
		sql.query("SELECT avg(datdiff(Departed,Arrived)) AS Tsrv FROM app.queues", [], function (err,stats) {
		sql.query("SELECT datediff(Now(),Arrived) AS Tage FROM app.queues ORDER BY Arrived ASC", [], function (err,load) {
			
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

FLEX.execute.issues = 
FLEX.execute.aspreqts = 
FLEX.execute.ispreqts = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

FLEX.delete.keyedit = function Xdelete(req, res) { 
	var sql = req.sql, query = req.query;
	
	Log(["delkey",req.group, query]);
	try {
		sql.query(
			"ALTER TABLE ??.?? DROP ??", 
			[req.group, query.ds, query.ID],
			function (err) {
				res( {data: {}, success:true, msg:"ok"}  );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
		
}

FLEX.insert.keyedit = function Xinsert(req, res) { 
	var sql = req.sql, body = req.body, query = req.query;
	
	Log(["addkey",req.group, query, body]);
	try {
		sql.query(
			"ALTER TABLE ??.?? ADD ?? "+body.Type,
			[req.group, query.ds, body.Key],
			function (err) {
				res( {data: {insertID: body.Key}, success:true, msg:"ok"} );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
}

FLEX.update.keyedit = function Xupdate(req, res) { 
	var sql = req.sql, body = req.body, query = req.query, body = req.body;
	
	Log(["updkey",req.group, query, body]);
	try {
		sql.query(
			"ALTER TABLE ??.?? CHANGE ?? ?? "+body.Type, 
			[req.group, query.ds, query.ID, query.ID],
			function (err) {
				res( {data: {}, success:true, msg:"ok"}  );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
	
}

FLEX.select.keyedit = function Xselect(req, res) { 
	var sql = req.sql, query = req.query;
	
	Log(["getkey",req.group, query]);
	
	try {
		sql.query("DESCRIBE ??.??", [req.group, query.ds || ""], function (err, parms) {
			if (err) return res(err);

			var recs = [{
				ID: "XXX", Ds: query.ds, Key: "new", Type: "int(11)", 
				Samples:0, Dist:"gaus", Parms:"[0,1]" 
			}];
			
			parms.each ( function (n,parm) {
				if ( parm.Field != "ID" )
					recs.push( {
							ID: parm.Field, Ds: query.ds, Key: parm.Field, Type: parm.Type, 
							Samples:0, Dist:"gaus", Parms:"[0,1]" } );
			});
			res(recs);
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
}

FLEX.execute.keyedit = function Xexecute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	Log(["exekey",req.group, query]);
	res("monted");
}

/*
FLEX.execute.parms = function Xexecute(req, res) { 
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
		Log(a+JSON.stringify(b));
		res();
	}

	res(SUBMITTED);
	
	var allparms = {}, ntables = 0;
	sql.eachTable("app", function (tname) {
		
		sql.query("DESCRIBE ??",[tname], function (err, parms) {

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
						var tablelist = FLEX.listify(tables).join(",");
						var ptype = "varchar";
						for (var tname in tables) ptype = tables[tname];
						
						var parm = {Tables:tablelist, Parm:pname, Type:ptype};

						sql.query("INSERT INTO parms SET ? ON DUPLICATE KEY UPDATE ?",[parm,parm]);
					});
			});
		
	});
	
	/ *if (true)
	sql.query("SHOW TABLES FROM app WHERE Tables_in_app1 NOT LIKE '\\_%'", function (err,tables) {	// sync parms table with DB
		var allparms = {}, ntables = 0;

		Trace("DESCRIBING TABLES");
		
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
						var tablelist = FLEX.listify(tables).join(",");
						var ptype = "varchar";
						for (var tname in tables) ptype = tables[tname];
						
						//Log([pname,ptype]);
						
						var parm = {Tables:tablelist, Parm:pname, Type:ptype};

						sql.query("INSERT INTO parms SET ? ON DUPLICATE KEY UPDATE ?",[parm,parm]);
					});
			});

		});
	}); * /
	
	if (false)
	sql.query("SELECT * FROM app.parms", function (err,parms) { // sync DB with parms table
		
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


FLEX.execute.attrs = function Xexecute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	var created = [];
		
	res(SUBMITTED);

	sql.query("SELECT * FROM openv.attrs")
	.on("result", function (dsattr) {		
		sql.query("CREATE TABLE ?? (ID float unique auto_increment)", dsattr.Table);
	});
}
*/

FLEX.execute.lookups = function Xexecute(req, res) {
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

/*
FLEX.execute.searches = function Xexecute(req, res) {
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
			Trace(err);
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
*/

/*
FLEX.execute.chips = function Xexecute(req, res) {   
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
*/

FLEX.execute.swaps = function Xexecute(req, res) {   
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);

	var form_name = "test1040",
		form_template = "~/sigma/shares/" + form_name + ".pdf";

	// poll repos for new packages
	
	sql.query("SELECT * FROM openv.swaps WHERE Enabled AND least(?,1) GROUP BY SW_Name", guardQuery(query,true) )
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

		Trace("UPGRADE "+package, sql);

		// submit swaps that have not already been submitted 
		
		sql.query("SELECT * FROM openv.swaps WHERE Enabled AND NOT Submitted")
		.on("result", function (swap) {
			sql.query("UPDATE swaps SET ? WHERE ?",[{Submitted: new Date()}, {ID:swap.ID}]);
			
			sendMail({
				to:  site.POC,
				subject: "SWAP "+package,
				body: `
Please find attached SWAP.  
Delivery of  "+swap.Product+" is conditional on NGA/OCIO acceptance of this deviation request.`
			}, sql);
			
		});
		
		return;
		
		// create a swap pdf
		
		sql.query("SELECT * FROM app.lookups WHERE ?",{Ref:form_name}, function (err,looks) {
			
			var form_map = {};
			
			looks.each(function (n,look) {
				if (look.Name)
					form_map[look.Name] = look.Path;
			});
				
			var swap_map = {};
			
			for (var n in form_map)
				swap_map[form_map[n]] = swap[n];

			/*PDF.fillForm( form_template, form_final, swap_map, function (err) {
				if (err) Log(`SWAP ${package} err`);
			});*/
			
			/*
			PDF.mapForm2PDF( swap, form_conv, function (err,swap_map) {
				Log("map="+err);
				
				PDF.fillForm( form_template, form_final, swap_map, function (err) {
					Log("fill="+err);
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
					Trace("UNPACK "+package, sql);
				});
			else {  // crawl site with phantomjs to pull latest source
			}

		});
		});
	});
	
}

/*
// legacy JSON editor for mysql cluster < 7.5

FLEX.select.json = 
FLEX.update.json = 
FLEX.delete.json = 
FLEX.insert.json = 

function (req,res) {
	
	var sql = req.sql,
		query = req.query,
		body = req.body,
		flags = req.flags,
		keys = (query.key||body.key||"nokey").split("."),
		db = "openv." + keys[0];

	delete query.key; 
	delete body.key;
	
Log({
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
*/

FLEX.execute.detectors = function Xexecute(req, res) { 
// execute(client,job,res): create detector-trainging job for client with callback to res(job) when completed.

	var sql = req.sql, log = req.log, query = req.query, job = req.body;

	var vers = [0]; //job.Overhead ? [0,90] : [0];
	var labels = job.Labels.split(",");

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
			"UPDATE app.proofs SET ? WHERE ? AND ?",
			[{posLock:job.Name}, {cat:"digit"}, {label:label}],
		//	"UPDATE proofs SET ? WHERE MATCH (label) AGAINST (? IN BOOLEAN MODE) AND enabled",
		//	[{posLock:job.Name},posFilter], 
			function (err) {

		sql.query(		// allocate negatives to this job
			"UPDATE app.proofs SET ? WHERE ? AND NOT ?",
			[{negLock:job.Name}, {cat:"digit"}, {label:label}],
		//	"UPDATE proofs SET ? WHERE MATCH (label) AGAINST (? IN BOOLEAN MODE) AND enabled",
		//	[{negLock:job.Name},negFilter], 
			function (err) {

		sql.query(
			"SELECT * FROM app.proofs WHERE ? LIMIT 0,?",		// get allocated positives
			[{posLock:job.Name},job.MaxPos],
			function (err,posProofs) {

		sql.query(								// get allocated negatives
			"SELECT * FROM app.proofs WHERE ? LIMIT 0,?",
			[{negLock:job.Name},job.MaxNeg],
			function (err,negProofs) {
				
		sql.query(			// end proofs lock.
			"COMMIT", 
			function (err) { 
		
		Trace("PROOF ",[posProofs.length,negProofs.length], sql);

		if (posProofs.length && negProofs.length) {	// must have some proofs to execute job
			
			var	
				posDirty = posProofs.sum("dirty"),
				negDirty = negProofs.sum("dirty"),
				totDirty = posDirty + negDirty,
				totProofs = posProofs.length + negProofs.length,
				dirtyness = totDirty / totProofs;

			Trace('DIRTY', [dirtyness,job.MaxDirty,posDirty,negDirty,posProofs.length,negProofs.length], sql);

			sql.query("UPDATE detectors SET ? WHERE ?",[{Dirty:dirtyness},{ID:job.ID}]);
			
			if (dirtyness >= job.MaxDirty) {		// sufficiently dirty to cause job to execute ?
				
				sql.query("UPDATE proofs SET dirty=0 WHERE least(?)",{posLock:job.Name,negLock:job.Name});
				
				vers.each( function (n,ver) {  		// train all detector versions
						
					var det = FLEX.clone(job);
					
					det.Path = "det"+ver+"/"+label+"/"; 		// detector training results placed here
					det.DB = "../db"+ver;						// positives and negatives sourced from here relative to ENV.DETS
					det.posCount = posProofs.length;
					det.negCount = negProofs.length;
					det.posPath = det.Path + "positives.txt"; 	// + ENV.POSITIVES + (false ? jobFolder + ".positives" : det.PosCases + ".jpg");  		// .positives will disable auto-rotations
					det.negPath = det.Path + "negatives.txt"; 	// + ENV.NEGATIVES + jobFolder + ".negatives";
					det.vecPath = det.Path + "samples.vec";
					det.posLimit = Math.round(det.posCount * 0.9); 	// adjust counts so haar trainer does not exhaust supply
					det.negLimit = Math.round(det.negCount * 1.0);
					
					det.link = det.Name.tag("a",{href:"/swag.view?goto=Detectors"}) + " " + det.posLimit + " pos " + det.negLimit + " neg";
					det.name = det.Name;
					det.client = log.client;
					det.work = det.posCount + det.negCount;

					Trace(`TRAIN ${det.Name} v${ver}`, sql);
				
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
						
					Trace((det.Execute||"").toUpperCase()+" "+det.name, sql);
					
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

							if (FLEX.CHIP)
							FLEX.CHIP.workflow(sql, {
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
									link: det.Name.tag("a",{href:"/swag.view?goto=Detectors"}),
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
 @private
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
		
	Trace("STAT REPO", sql);
	
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

function feedNews(sql, engine) {
	sql.query("SELECT * FROM openv.feeds WHERE NOT ad")
	.on("result", function (feature) {
		NEWSFEED.addItem({
			title:          feature.feature,
			link:           `${FLEX.paths.host}/feed.view`,
			description:    JSON.stringify(feature),
			author: [{
				name:   FLEX.site.title,
				email:  FLEX.mailer.SOURCE,
				link:   FLEX.paths.host
			}],
			/*contributor: [{
				name:   FLEX.TITLE,
				email:  site.email.SOURCE,
				link:   FLEX.paths.host
			}],*/
			date:           feature.found
			//image:          posts[key].image
		});
	});	
    
	NEWSFEED.render("rss-2.0");  // or "atom-1.0"

	sql.query("UPDATE features SET Ad=1 WHERE NOT ad");
    
	READ(FLEX.NEWREAD.URL, function(err, articles) {
		if (err)
			console.info("Ignoring news reader "+FLEX.NEWREAD.URL);
		else
			articles.each( function (n,article) {
				// "title"     - The article title (String).
				// "author"    - The author's name (String).
				// "link"      - The original article link (String).
				// "content"   - The HTML content of the article (String).
				// "published" - The date that the article was published (Date).
				// "feed"      - {name, source, link}
				
				// if this is a foreign feed, we can set args to ("change", associated file)
				if (FLEX.NEWREAD.JOB)
					FLEX.NEWREAD.JOB( sql, "feed", article.link);
			});
	});
}

/*
 @method requestService(srv,cb)
 * 
 * Issue http/https request to the desiired service srv (host,port,path) using 
 * the specified srv method. The response is routed to the callback cb.
 * Known nodejs bug prevents connections to host = "localhost".
 * */
/*
function requestService(srv, cb) {

	var req = HTTP.request(srv, function(cb) {
		
		Log(`STATUS: ${cb.statusCode}`);
		Log(`HEADERS: ${JSON.stringify(cb.headers)}`);
		
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
		Log("http apperr="+err);
	});

	//req.write('data\n');	// write data to request body
	req.end();				// end request
}
*/

// Email support methods

function showIMAP(obj) {
//  return inspect(obj, false, Infinity);
}

function killIMAP(err) {
	Log(err);
	process.exit(1);
}

function openIMAP(cb) {
	FLEX.mailer.RX.TRAN.connect(function(err) {  // login cb
		if (err) killIMAP(err);
		FLEX.mailer.RX.TRAN.openBox('INBOX', true, cb);
	});
}

function sendMail(opts,sql) {
	
	Trace(`MAIL ${opts.to} RE ${opts.subject}`, sql);

	opts.from = "totem@noreply.gov";
	opts.alternatives = [{
		contentType: 'text/html; charset="ISO-59-1"',
		contents: ""
	}];

	if (opts.to) 
		if (x = FLEX.mailer)
			if (x = x.TX.TRAN) 
				x.sendMail(opts,function (err) {
					//Trace("MAIL "+ (err || opts.to) );
				});
		
			else
				CP.exec(`echo -e "${opts.body||'FYI'}\n" | mail -r "${opts.from}" -s "${opts.subject}" ${opts.to}`, function (err) {
					//Trace("MAIL "+ (err || opts.to) );
				});
}

/**
 @method flattenCatalog
 Flatten entire database for searching the catalog
 * */
function flattenCatalog(flags, catalog, limits, cb) {
	
	function flatten( sql, rtns, depth, order, catalog, limits, cb) {
		var table = order[depth];
		
		if (table) {
			var match = catalog[table];
			var filter = cb.filter(match);
			
			var quality = " using "+ (filter ? filter : "open")  + " search limit " + limits.records;
			
			Trace("CATALOG "+table+quality+" RECS "+rtns.length, sql);
		
			var query = filter 
					? "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID, " + filter + " FROM ?? HAVING Score>? LIMIT 0,?"
					: "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID FROM ?? LIMIT 0,?";
					
			var args = filter
					? [table, limits.score, limits.records]
					: [table, limits.records];

			Trace( sql.query( query, args,  function (err,recs) {
				
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

					flatten( sql, rtns, depth+1, order, catalog, limits, cb );
				}
				else 
					sql.query("select found_rows()")
					.on('result', function (stat) {
						
						recs.each( function (n,rec) {						
							rtns.push( {
								ID: rtns.length,
								Ref: table,
								Name: `${table}.${rec.ID}`,
								Dated: limits.stamp,
								Quality: recs.length + " of " + stat["found_rows()"] + quality,
								Link: table.tag("a",{href: "/" + table + ".db?ID=" + rec.ID}),
								Content: JSON.stringify( rec )
							} );
						});

						flatten( sql, rtns, depth+1, order, catalog, limits, cb );
					});
			}) );	
		}
		else
			cb.res(rtns);
	}

	/*
	function escape(n,arg) { return "`"+arg+"`"; }
	*/
	
	var sql = this,
		rtns = [];
		/*limits = {
			records: 100,
			stamp: new Date()
			//pivots: flags._pivot || ""
		};*/
		
	flatten( sql, rtns, 0, FLEX.listify(catalog), catalog, limits, {
		res: cb, 

		filter: function (search) {
			return ""; //Builds( "", search, flags);  //reserved for nlp, etc filters
	} });
}

function hawkCatalog(req,res) {
	var sql = this,
		flags = req.flags;
	
	function queueExe(cls,name,exe) {
		
		sql.insertJob({
			class: cls,
			client: req.client,
			qos: req.profile.QoS,
			priority: 0,
			//key: name,
			req: Copy(req,{}),
			name: name
		}, exe);
	}
	
	Trace(`HAWK CATALOG FOR ${req.client}`);
	
	if (has = flags.has)
		queueExe("detector", has, function (req,res) {
			//Log("create detector "+req.has);
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

//=========== Job queue interface
/*
 * Job queue interface
 * 
 * select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
 * execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
 * update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
 * delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
 * insert(job,cb): add job and route to callback cb(job) when executed.
 */

FLEX.queues = {};
	
/*
@method selectJob
@param {Object} req job query
@param {Function} cb callback(rec) when job departs
*
* Callsback cb(rec) for each queuing rec matching the where clause.
* >>> Not used but needs work 
 */
function selectJob(where, cb) { 

	// route valid jobs matching sql-where clause to its assigned callback cb(req).
	var sql = this;
	
	sql.query(
		where
		? `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client WHERE ${where} ORDER BY QoS,Priority`
		: `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client ORDER BY QoS,Priority`
	)
	.on("error", function (err) {
		Log(err);
	})
	.on("result", function (rec) {
		cb(rec);
	});	
}

/*
@method updateJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
*
* Adjust priority of jobs matching sql-where clause and route to callback cb(req) when updated.
* >>> Not used but needs work 
*/
function updateJob(req, cb) { 
	
	var sql = this;
	
	sql.selectJob(req, function (job) {
		
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
				delete FLEX.queues[job.qos].batch[job.ID];
				job.qos = req.qos;
				FLEX.queues[qos].batch[job.ID] = job;
			}
			
			if (req.pid)
				CP.exec(`renice ${req.inc} -p ${job.pid}`);				
				
		});
	});
}
		
/*
@method deleteJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
* >>> Not used but needs work
*/
function deleteJob(req, cb) { 
	
	var sql = this;
	sql.selectJob(req, function (job) {
		
		cb(sql,job, function (ack) {
			sql.query("UPDATE queues SET Departed=now(), Age=(now()-Arrived)/3600e3, Notes=concat(Notes,'stopped') WHERE ?", {
				Task:job.task,
				Client:job.client,
				Class:job.class,
				QoS:job.qos
			});

			delete FLEX.queues[job.qos].batch[job.priority];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

function insertJob(job, cb) { 
/*
@method insertJob
@param {Object} job arriving job
@param {Function} cb callback(sql,job) when job departs

Adds job to the specified (client,class,qos,task) queue.  A departing job will execute the supplied 
callback cb(sql,job) on a new sql thread (or spawn a new process if job.cmd provided).  The job
is regulated by its job.rate [s] (0 disables regulation). If the client's job.credit has been exhausted, the
job is added to the queue, but not to the regulator.  Queues are periodically monitored to store 
billing information.  When using insertJob within an async loop, the caller should pass a cloned copy
of the job.
 */
	function cpuavgutil() {				// compute average cpu utilization
		var avgUtil = 0;
		var cpus = OS.cpus();
		
		cpus.each(function (n,cpu) {
			idle = cpu.times.idle;
			busy = cpu.times.nice + cpu.times.sys + cpu.times.irq + cpu.times.user;
			avgUtil += busy / (busy + idle);
		});
		return avgUtil / cpus.length;
	}
	
	function regulate(job,cb) {		// regulate job (spawn if job.cmd provided)
			
		var queue = FLEX.queues[job.qos];	// get job's qos queue
		
		if (!queue)  // prime the queue if it does not yet exist
			queue = FLEX.queues[job.qos] = new Object({
				timer: 0,
				batch: {},
				rate: job.qos,
				client: {}
			});
			
		var client = queue.client[job.client];  // update client's bill
		
		if ( !client) client = queue.client[job.client] = new Object({bill:0});
		
		client.bill++;

		var batch = queue.batch[job.priority]; 		// get job's priority batch
		
		if (!batch) 
			batch = queue.batch[job.priority] = new Array();

		batch.push( Copy(job, {cb:cb, holding: false}) );  // add job to queue
		
		if ( !queue.timer ) 		// restart idle queue
			queue.timer = setInterval(function (queue) {  // setup periodic poll for this job queue

				var job = null;
				for (var priority in queue.batch) {  // index thru all priority batches
					var batch = queue.batch[priority];

					job = batch.pop(); 			// last-in first-out

					if (job) {  // there is a departing job 
//Log("job depth="+batch.length+" job="+[job.name,job.qos]);

						if (job.holding)  // in holding / stopped state so requeue it
							batch.push(job);
									   
						else
						if (job.cmd) {	// this is a spawned job so spawn and hold its pid
							job.pid = CP.exec(
									job.cmd, 
									  {cwd: "./public/dets", env:process.env}, 
									  function (err,stdout,stderr) {

								job.err = err || stderr || stdout;

								if (job.cb) job.cb( job );  // execute job's callback
							});
						}
					
						else  			// execute job's callback
						if (job.cb) job.cb(job);

						break;
					}
				}

				if ( !job ) { 	// an empty queue goes idle
					clearInterval(queue.timer);
					queue.timer = null;
				}

			}, queue.rate*1e3, queue);
	}

	var 
		sql = this;
	
	if (job.qos)  // regulated job
		sql.query(  // insert job into queue or update job already in queue
			"INSERT INTO app.queues SET ? ON DUPLICATE KEY UPDATE " +
			"Departed=null, Work=Work+1, State=Done/Work*100, Age=(now()-Arrived)/3600e3, ?", [{
				// mysql unique keys should not be null
				Client: job.client || "",
				Class: job.class || "",
				Task: job.task || "",
				QoS: job.qos || 0,
				// others 
				State: 0,
				Arrived	: new Date(),
				Departed: null,
				Marked: 0,
				Name: job.name,
				Age: 0,
				Classif : "",
				//Util: cpuavgutil(),
				Priority: job.priority || 0,
				Notes: job.notes,
				Finished: 0,
				Billed: 0,
				Funded: job.credit ? 1 : 0,
				Work: 1,
				Done: 0
			}, {
				Notes: job.notes,
				Task: job.task || ""
			}
		], function (err,info) {  // increment work backlog for this job

			//Log([job,err,info]);
			
			if (err) 
				return Log(err);
			
			job.ID = info.insertId || 0;
			
			if (job.credit)				// client still has credit so place it in the regulators
				regulate( job , function (job) { // provide callback when job departs
					FLEX.thread( function (sql) {  // callback on new sql thread
						cb(sql,job);

						sql.query( // reduce work backlog and update cpu utilization
							"UPDATE app.queues SET Age=now()-Arrived,Done=Done+1,State=Done/Work*100 WHERE ?", [
							// {Util: cpuavgutil()}, 
							{ID: job.ID} //jobID 
						]);
	
						sql.release();
						/*
						sql.query(  // mark job departed if no work remains
							"UPDATE app.queues SET Departed=now(), Notes='finished', Finished=1 WHERE least(?,Done=Work)", 
							{ID:job.ID} //jobID
						);
						*/
					});
				});
		});

	else  { // unregulated so callback on existing sql thread
		job.ID = 0;
		cb(sql, job);
	}
}
	
function executeJob(req, exe) {

	function flip(job) {  // flip job holding state
		if ( queue = FLEX.queues[job.qos] ) 	// get job's qos queue
			if ( batch = queue.batch[job.priority] )  // get job's priority batch
				batch.each( function (n, test) {  // matched jobs placed into holding state
					if ( test.task==job.task && test.client==job.client && test.class==job.class )
						test.holding = !test.holding;
				});
	}
	
	var sql = req.sql, query = req.query;
	
	sql.query("UPDATE ??.queues SET Holding = NOT Holding WHERE ?", {ID: query.ID}, function (err) {
		
		if ( !err )
			flip();
	});
}

/*
function selectData(req, ds, cb) {
	this.query("SELECT Data FROM contexts WHERE least(?,1) LIMIT 0,1", {
		Client: req.client,
		Dataset: req.table
	}, function (err,recs) {
		var def = {};
		
		if (err)
			cb( def );
		else
			try {
				cb( JSON.parse(recs[0].Data) );
			}
			catch (err) {
				cb( def );
			});		
	});		
}
function insertData(req, ds) {
}
function deleteData(req, cb) {
}
function updateData(req, ds) {
	this.query("UPDATE contexts SET ? WHERE least(?,1)", [ds, {		
		Client: req.client,
		Dataset: req.table
	}]);		
}
function executeData(req, cb) {
}
*/

//============  Database CRUDE interface

function queryDS(req, res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		query = req.query,
		body = Copy(query,req.body);

	delete body.ID;

	sql.context({ds: {
			table:	(FLEX.dbRoutes[req.table] || req.group)+"."+req.table,
			where:	flags.where || query,
			res:	res,
			order:	flags.sort,
			having: flags.having,
			group: 	flags.group || flags.tree,
			score:	flags.score,
			limit: 	flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
			index: 	{
				has:flags.has, 
				nlp:flags.nlp, 
				bin:flags.bin, 
				qex:flags.qex, 
				browse:flags.browse, 
				pivot:flags.pivot, 
				select: flags.index ? flags.index.split(",") : "*"
			},
			data:	body,
			client: req.client
		}}, function (ctx) {
			
		ctx.ds.rec = (flags.lock ? "lock." : "") + req.action;
		
	});
}

//============ misc 

FLEX.select.agent = function (req,res) {
	var
		sql = req.sql,
		query = req.query,
		push = query.push,
		pull = query.pull;
	
	if (push) 
		CRYPTO.randomBytes(64, function (err, jobid) {

			try {
				var args = JSON.parse(query.args);
			}
			catch (parserr) {
				err = parserr;
			}
			
			if (err) 
				res( "" );

			else
				res( jobid.toString("hex") );

		});
	
	else
	if (pull) {
		var jobid = query.jobid;
		
		if (jobid) 
			res( {result: 123} );
		
		else
			res( "Missing jobid" );
	}
	else
		res( "Missing push/pull" );
	
}

/*
FLEX.select.quizes = function (req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	if (query.lesson)
		sql.query(  // prime quiz if it does not already exists
			"INSERT INTO app.quizes SELECT * FROM app.quizes WHERE least(?,1)", {
				Client: "Teacher",
				Lesson: query.lesson
			}, function (err) {

				sql.query(  // clear last results
					"UPDATE app.quizes SET ? WHERE least(?,1)", [{
						A: "",
						C: "",
					}, {
						Client: req.client,
						Lesson: query.lesson
					}], function (err) {

						sql.query(  // return client's quiz 
							"SELECT * FROM app.quizes WHERE least(?,1)", {
									Client: req.client
							}, function (err,recs) {
								res(err || recs);
							});

					});
			});
	
	else
		sql.query(
			"SELECT * FROM app.quizes WHERE least(?,1)", {
					Client: req.client
			}, function (err,recs) {
				res(err || recs);
			});
}

FLEX.execute.quizes = function (req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	if (query.lesson)
		sql.query(
			"SELECT * FROM app.quizes WHERE least(?,1)", {
				Client: "Teacher",
				Lesson: query.lesson
			}, function (err,ans) {
				res("Refresh to get scores");
			})
		
	else
		res("Mission lesson");
}
*/

FLEX.select.follow = function (req,res) {  // follow a link
	var 
		sql = req.sql, 
		query = req.query;
	
	res("ok");
	
	sql.query("INSERT INTO app.follows SET ? ON DUPLICATE KEY UPDATE Count=Count+1,Event=now()", {
		Goto: query.goto.split("?")[0], 
		Client: query.client,
		View: query.view,
		Event: new Date(),
		Count: 1
	});
	
}

function Trace(msg,sql) {
	ENUM.trace("X>",msg,sql);
}

function beginBulk() {
	this.query("START TRANSACTION");
	this.query("SET GLOBAL sync_binlog=0");
	this.query("SET GLOBAL innodb-flush-log-at-trx-commit=0");
}

function endBulk() {
	this.query("COMMIT");
	this.query("SET GLOBAL sync_binlog=1");
	this.query("SET GLOBAL innodb-flush-log-at-trx-commit=1");
}

FLEX.select.login = function(req,res) {
	var 
		sql = req.sql, 
		query = req.query,
		site = FLEX.site,
		url = site.urls.worker,
		nick = site.nick,
		nickref = nick.tag("a",{href:url}),
		client = req.client,
		user = userid(client),
		group = req.group,
		profile = req.profile,
		pass = query.pass,
		userHome = `/local/users/${user}`,
		sudoJoin = 'echo "ILEbooboo9999#2" | sudo -S ',
		aws = {
			machine: "tbd.tbd.tbd",
			admin: "brian.d.james@coe.ic.gov",
			ps: "brian.d.james@coe.ic.gov",
			remotein: `./shares/${user}.rdp`,
			sudos: [
				`adduser ${user} -M --gid ${group} -p ${pass}`,
				`usermod -d ${userHome} ${user}`,
				`id ${user}`,
				`mkdir -p ${userHome}/.ssh`,
				`cp ./certs/${user} ${userHome}/.ssh`,
				`cp ./shares/template.rdp ./shares/${user}.rdp`,
				`ln -s /local/service ${userHome}/totem`
			],				
			hosted: false
		},
		notice = `
Greetings from ${nickref}-

Your TOTEM development login ${user} has been created for ${client}.

${aws.admin}: Please create an AWS EC2 account ${user} for ${client} using the attached cert.

To connect to ${nickref} from Windows:

1. Establish a putty gateway using the following Local,Remote port map (set in putty | SSH | Tunnels):

		5001, ${url}:22
		5100, ${url}:3389
		5200, ${url}:8080
		5910, ${url}:5910
		5555, Dynamic

	and, for convienience:

		Pageant | Add Keys | your private ppk cert

2. Start a ${nickref} session using one of these methods:

	Putty | Session | Host Name = localhost:5001 
	Remote Desktop Connect| Computer = localhost:5100 
	FF | Options | Network | Settings | Manual Proxy | Socks Host = localhost, Port = 5555, Socks = v5 ` ;
	
	if (pass && client != "guest")
		if ( profile.User )
			res( function () { return aws.remotein; } );
	
		else
		if ( createCert = FLEX.createCert )
			createCert(user, pass, function () {
				var 
					prep = aws.sudos.join(";"),
					prepenv = sudoJoin + aws.sudos.join("; "+sudoJoin);

				Trace(`CREATE CERT FOR ${client} PREP ${prep}`, sql);

				CP.exec( prepenv, function (err,out) {
					if (err)  {
						res( FLEX.errors.failedLogin );
						sendMail({
							to: aws.admin,
							cc: aws.ps,
							subject: `${nick} login failed`,
							body: err + `This is an automatted request from ${nick}.  Please provide ${aws.ps} "sudo" for ${prep} on ${aws.machine}`
						});
					}

					else  {
						res( function () { return aws.remotein; } );
						sql.query("UPDATE openv.profiles SET ? WHERE ?", [{User: user}, {Client: client}]);
						sendMail({
							to: client,
							cc: [aws.ps,aws.admin].join(";"),
							subject: `${nick} login established`,
							body: notice
						});
					};
				});
			});
	
		else	
			res( FLEX.errors.failedLogin );
	
	else
		res( FLEX.errors.badLogin );
}

function userid(client) {
	var 
		parts = client.split("@"),
		parts = (parts[0]+".x.x").split("."),
		user = parts[0].charAt(0)+parts[1].charAt(0)+parts[2];
	
	return user.substr(0,8).toLowerCase();
}

FLEX.select.proctor = function (req,res) {
	var 
		site = FLEX.site,
		sql = req.sql,
		query = req.query,
		client = req.client,
		parts = query.lesson.split("."),
		topic = parts[0],
		mod = parseInt( parts[1] ) || 1,
		set = parseInt( parts[2] ) || 1,
		mods = query.modules || 1,
		passed = query.score >= query.pass;

	//Log(query.score, query.pass, [topic, set, mod, mods].join("."));
		
	sql.query("INSERT INTO app.quizes SET ? ON DUPLICATE KEY UPDATE Tries=Tries+1,?", [{
		Client: client,
		Score: query.score,
		Topic: topic,
		Module: mod,
		Set: set,
		Pass: query.pass,
		Taken: new Date(),
		Tries: 1
	}, {
		Score: query.score,
		Pass: query.pass,
		Taken: new Date()
	}], function (err) {
		
		sql.all(
			"",
			"SELECT count(ID) AS Count FROM app.quizes WHERE Score>Pass AND least(?) GROUP BY Module", 
			{Client:client, Topic:topic}, 
			function (recs) {
				
				var certified = recs.length >= mods;
				
				res( `Thank you ${client} - ` + (
					certified 
					? "you completed all modules - your certificate of completion will be emailed"
					
					: passed 
						? `you passed set ${set} of module ${mod}-${mods}!`
						: "please try again" 
				));
				
				if ( certified ) {
					sql.query(
						"UPDATE app.quizes SET Certified=now() WHERE least(?)", 
						{Client:client, Topic:topic} );
					
					sendMail({
						to: client,
						subject: `${site.nick} sends its congradulations`,
						body: 
							`you completed all ${topic} modules and may claim your `
							+ "certificate".tag("a", {href:`/stores/cert_${topic}_${client}.pdf`})
					});
				}
				
		});
	
	});
};
		
// UNCLASSIFIED
