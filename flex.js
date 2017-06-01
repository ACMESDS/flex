// UNCLASSIFIED

/*
 * @class flex
 * @requires vm
 * @requires http
 * @requires crypto
 * @requires url
 * @requires cluster
 * @requires fs
 * @requires child-process
 * @requires os

 * @requires enum
 * @requires engine

 * @requires pdffiller
 * @requires nodemailer
 * @requires imap
 * @requires feed
 * @requires feed-read
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

var 											// globals
	ENV = process.env, 					// external variables
	SUBMITTED = "submitted";

var 											// totem bindings
	READ = require("../reader"),
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
		console.log("F>"+msg);
	else
	if (msg.constructor == Error)
		console.log("F>ERROR",msg);
	
	else
		console.log("F>"+msg.sql);

	if (arg) console.log(arg);
		
	return msg;
}

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
			protectedQueue: new Error("action not allowed on this job queues")
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
		
		watch: null,
		
		// CRUDE interface
		select: {ds: runQuery}, 
		delete: {ds: runQuery}, 
		update: {ds: runQuery}, 
		insert: {ds: runQuery}, 
		execute: {ds: runQuery}, 
	
		fetcher: null, 					// http data fetcher
		uploader: null,		 			// file uploader
		emitter: null,		 			// Emitter to sync clients
		thread: null, 					// FLEX connection threader
		skinner : null, 				// Jade renderer
		
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
			
			if (opts) Copy(opts,FLEX);
			
			if (watch = FLEX.watch) 
			FS.readdir( watch, function (err, files) {
				files.each(function (n,file) {
					Trace("WATCHING "+file);
					FS.watch(watch + file, function (ev, file) {  //{persistent: false, recursive: false}, 

						//console.log([ev,file, FLEX.thread]);
						Trace(ev.toUpperCase()+" "+file);

						if (file && FLEX.thread)
						switch (ev) {
							case "change":
								FLEX.thread( function (sql) {
									READ.reader(sql, watch+file, function (keys) {
										console.log(["keys",keys]);
									});
									sql.release();
								});
								
								break;

							case "x":
							default:
							
						}
					});
				});
			});				
				
			if (FLEX.thread)
			FLEX.thread( function (sql) {
				
				Trace("EXTENDING SQL CONNECTOR");
				
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
					runQuery, 
					flattenCatalog,
					hawkJobs
				]);

				sql.hawkJobs("flex", FLEX.site.masterURL);

				READ.config(sql);			
				
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

				// setup likeus table hawk
				if (FLEX.likeus.PING)
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

				/*
				if (opts.pulse.PING)
					setInterval( function() {
						
						FLEX.thread(function (sql) {

							sql.query("SELECT count(ID) AS Count FROM engines WHERE Enabled")
							.on("result", function (engs) {
							sql.query("SELECT count(ID) AS Count FROM queues WHERE Departed IS NULL")
							.on("result", function (jobs) {
							sql.query("SELECT sum(DateDiff(Departed,Arrived)>1) AS Count from queues")
							.on("result", function (pigs) {
							sql.query("SELECT sum(Delay>20)+sum(Fault != '') AS Count FROM dblogs")
							.on("result", function (isps) {
								var rtn = FLEX.pulse.COUNTS = {Engines:engs.Count,Jobs:jobs.Count,Pigs:pigs.Count,Faults:isps.Count,State:"ok"};
								var lims = FLEX.pulse.LIMITS;
								
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
						
					}, opts.pulse.PING*(60*1000) );
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

						Trace(`MAILHOST  ${email.TX.HOST} PORT ${email.TX.PORT}`);

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

					if (FLEX.mailer.ONSTART) 
						sendMail({
							to: site.distro.hawk,
							subject: site.title + " started", 
							html: "Just FYI",
							alternatives: [{
								contentType: 'text/html; charset="ISO-59-1"',
								contents: ""
							}]
						}, function (err,info) {
							if (err) Trace(err);
						});
				}
			}			
		}

};

// Job queue CRUDE interface
/*
FLEX.select.jobs = function Select(req, res) { 
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

// SQL engines interface

FLEX.select.sql = function Select(req, res) {
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

FLEX.insert.sql = function Insert(req, res) {
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

FLEX.delete.sql = function Delete(req, res) {
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

FLEX.update.sql = function Update(req, res) {
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

FLEX.execute.git = function Execute(req,res) {  // baseline changes

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
	Trace("BASELINE GROUP "+(err||"OK"));

	CP.exec(ex.openv, function (err,log) {
	Trace("BASELINE OPENV "+(err||"OK"));

	CP.exec(ex.clear, function (err,log) {
	Trace("BASELINE CLEAR "+(err||"OK"));

	CP.exec(ex.prime, function (err,log) {
	Trace("BASELINE PRIME "+(err||"OK"));

	CP.exec(ex.rebase, function (err,log) {
	Trace("BASELINE REBASE "+(err||"OK"));

	CP.exec(ex.commit, function (err,log) {
	Trace("BASELINE COMMIT "+(err||"OK"));

		if (!query.noexit) process.exit();				
		
	});
	});
	});
	});
	});
	});
	
}

FLEX.select.git = function Select(req, res) {

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

// email peer-to-peer exchange interface
	
FLEX.select.email = function Select(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	sql.query("SELECT * FROM emails WHERE Pending", function (err,recs) {
		res(err || recs);

		sql.query("UPDATE emails SET Pending=0 WHERE Pending");
	});
	
}
	
FLEX.execute.email = function Execute(req,res) {
	var sql = req.sql, log = req.log, query = req.query, flags = req.flags;
	
	res(SUBMITTED);
	
	if (false)
	requestService(srv, function (rec) {
		sendMail({
			to:  rec.To,
			subject: rec.Subject,
			html: rec.Body,
			alternatives: [{
				contentType: 'text/html; charset="ISO-59-1"',
				contents: ""
			}]
		}, function (err,info) {
			if (err) Trace(err);
		});
	});
	
}

// Master catalog interface

/*FLEX.select.CATALOG = function Select(req, res) {
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

FLEX.execute.catalog = function Execute(req, res) {
	var 
		sql = req.sql, log = req.log, query = req.query, flags = req.flags,
		catalog = FLEX.flatten.catalog || {},
		limits = FLEX.flatten.limits || {};
	
	limits.pivots = flags.pivot || "";
	limits.stamp = new Date();

	if (false)
		console.log({
			cat: catalog,
			lim: limits
		});
	
	res(SUBMITTED);
	
	sql.query("DELETE from catalog");
	
	sql.flattenCatalog( flags, catalog, limits, function (recs) {
		
		Trace("FLATTEN "+recs.length);
		
		recs.each( function (n,rec) {
			sql.query("INSERT INTO catalog SET ?",rec);
		});

	});
	
}

// Getters

FLEX.select.ACTIVITY = function Select(req, res) {
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

FLEX.select.VIEWS = function Select(req, res) {
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

FLEX.select.LINKS = function Select(req, res) {
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

	FLEX.indexer( path, function (files) {

		files.each(function (n,file) {
		
			var stats = FS.statSync(path + file);
			
			if (stats.isDirectory()) 
				if (file.charAt(0) == ".") 
					FLEX.indexer( path+file, function (names) {
						
						names.each(function (n,name) {
							links.push({
								ID: id++,
								Name: (file.substr(1)+" &rrarr; "+name).tag("a",{
									href: `${name}.view`
										/*(FLEX.statefulViews[name] ? FLEX.WORKER : "") + area + name +".view"*/
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
							/*(FLEX.statefulViews[name] ? FLEX.WORKER : "") + area + file*/
						})
					});
			}
			
		});
	});
	
	res(links);
}

FLEX.select.THEMES = function (req, res) {
	var themes = [];
	var path = ENV.THEMES;

	FLEX.indexer( path , function (files) {
		files.each( function (n,file) {
			var stats = FS.statSync(path + "/"+file);

			if (!stats.isDirectory())
				themes.push( {ID: n, Name: file, Path: file} );
		});
	});
	
	res(themes);	
}

FLEX.select.SUMMARY = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;	
	var cnts = FLEX.pulse.COUNTS;
	
	// Could check rtn.serverStatus and warningCount (become defined when a MySQL table corrupted).  
	// Return selected record info (fieldCount, affectedRows, insertId, serverStatus, warningCount, msg)
				
	res(cnts.State
		? [ {ID:0, Name: cnts.State},
			{ID:1, Name: JSON.stringify(cnts).replace(/"/g,"").tag("font",{color: (cnts.State=="ok") ? "green" : "red"})}
		  ]
		: [ {ID:0, Name: "pending"} ]
	);
}

FLEX.select.USERS = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,Connects,userinfo(Client,Org,Location) AS Name FROM openv.sockets WHERE least(?,1) ORDER BY Client", 
		guardQuery(query,true),
		function (err, recs) {
			res(err || recs);
	});
}

FLEX.select.ENGINES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,engineinfo(Name,Engine,Updated,Classif,length(Code),Period,Enabled,length(Special)) AS Name FROM engines WHERE least(?,1) ORDER BY Name",
		guardQuery(query,true), 
		function (err, recs) {
			res(err || recs);
	});
}

FLEX.select.CONFIG = function Select(req, res) {
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
	
FLEX.select.TABLES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var rtns = [], ID=0;
	
	sql.indexTables( "app1", function (table) {
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

FLEX.select.ADMIN = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT *,avg_row_length*table_rows AS Used FROM information_schema.tables", [], function (err, recs) {
		res(err || recs);
	});
}
	
FLEX.select.QUEUES = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,queueinfo(Job,min(Arrived),timediff(now(),min(Arrived))/3600,Client,Class,max(State)) AS Name FROM queues GROUP BY Client,Class", 
		[],
		function (err, recs) {
			res(err || recs);
	});
}

FLEX.select.CLIQUES = function v(req, res) {
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

FLEX.select.HEALTH = function Select(req, res) {
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
		+ "round(avg(Transfer/Delay)*1e-3,2) AS avg_KBPS, "
		+ "round(max(Transfer/Delay)*1e-3,2) AS max_KBPS, "
		+ "sum(Fault !='') AS faults, "
		+ "round(sum(Transfer)*1e-9,2) AS tot_GB, "
		+ "count(ID) AS logs "
		+ "FROM dblogs")
	.on("error", function (err) {
		Trace(err);
	})
	.on("result", function (lstats) {

//console.log(lstats);

	sql.query(
		  "SELECT "
		+ "sum(departed IS null) AS backlog, "
		+ "avg(datediff(ifnull(departed,now()),arrived)) AS avg_wait_DAYS, "
		+ "sum(Age)*? AS cost_$ "
		+ "FROM queues",[4700])
	.on("error", function (err) {
		Trace(err);
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
			rtns.push({ID:ID++, Name:asp+" "+n.tag("a",{href:"/admin.jade?goto=SWAPs"})+" "+stat});
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

FLEX.select.likeus = function Select(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

console.log(FLEX.site);
	
	sendMail({
		to:  FLEX.site.distro.hawk,
		subject: req.client + " likes " + FLEX.site.title + " !!",
		html: "Just FYI",
		alternatives: [{
			contentType: 'text/html; charset="ISO-59-1"',
			contents: ""
		}]
	}, function (err,info) {
		if (err) Trace(err);
	});
	
	sql.query(
		"INSERT INTO openv.profiles SET ? " +
		"ON DUPLICATE KEY UPDATE Challenge=0,Likeus=Likeus+1,QoS=least(Likeus+1,5)", { 
		LikeUs:0, 
		QoS:0,
		Banned:"",
		Joined: new Date(),
		Updated: new Date(),
		Challenge:1,
		Client:req.client 
	})
	.on("error", function (err) {
		Trace(err);
	});

	res( `Thanks ${req.client} for liking ${FLEX.site.nick} !  Check out your new ` +
		"QoS profile".tag("a",{href:'/profile.jade'}) + " !" );

}

FLEX.select.tips = function Select(req, res) {
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
				Trace("tips",[err,q.sql]);
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

FLEX.select.history = function (req,res) {
	var sql = req.sql, log = req.log, query = req.query;

	var pivots = {
		"both": "journal.Dataset,journal.Field",
		bydataset: "journal.Dataset",
		byfield: "journal.Field"
	},
		pivot = pivots[query.pivot || "bydataset"] || pivots.both;
	
	sql.query("USE openv", function () {
		
		switch (query.option) {

			case "signoffs":
				//var comment = "".tag("input",{type:"file",value:"mycomments.pdf",id:"uploadFile", multiple:"multiple",onchange: "BASE.uploadFile()"});
				
				Trace(sql.query(
					"SELECT "
					+ "Hawk, max(power) AS Power, "
					//+ "? AS Comment, "
					//+ 'approved' AS Comment, "
					//+ "link(concat('Files|Upload|', Hawk, '.pdf'), concat('/uploads/', Hawk, '.pdf')) AS DetailedComments, "
					+ "group_concat(distinct ifnull(link(journal.dataset,concat('/',viewers.viewer,'.view')),journal.dataset)) AS Changes,"
					+ "link('Approve', concat('/history.db', char(63), 'option=approve'," 
					+ 		" '&Power=', Power,"
					+ 		" '&Hawk=', Hawk,"
					+ 		" '&Datasets=', group_concat(distinct journal.dataset))) AS Approve,"
					+ "group_concat(distinct journal.field) AS Fields,"
					+ "linkrole(hawk,max(updates),max(power)) AS Moderator,"
					+ "false as Approved "
					+ "FROM journal "
					+ "LEFT JOIN viewers ON journal.Dataset=viewers.Dataset "
					+ "WHERE Updates "
					+ `GROUP BY hawk,${pivot}`,
					function (err, recs) {
						
						res( err || recs );
					}));
				break;
					
			case "changes":

				Trace(sql.query(
				"SELECT "
				+ "group_concat(distinct concat(Dataset,'.',Field)) AS Datasets,"
				+ "group_concat(distinct linkrole(Hawk,Updates,Power)) AS Moderators "
				+ "FROM journal "
				+ "WHERE Updates "
				+ `GROUP BY ${pivot}`, 
					function (err, recs) {
					
					res( err || recs );
				}));
				break;
				
			case "earnings":
				Trace(sql.query(
					"SELECT Client, group_concat(distinct year(Reviewed),'/',month(Reviewed), ' ', Comment) AS Comments, "
					+ "group_concat( distinct linkrole(roles.Hawk,roles.Earnings,roles.Strength)) AS Earnings "
					+ "FROM roles "
					+ "LEFT JOIN journal ON roles.Hawk=journal.Hawk "
					+ "WHERE Updates "
					+ "GROUP BY Client, roles.Hawk", 
					function (err, recs) {
					
					res( err || recs );
				}));
				break;
				
			case "approve":
				res( "Thank you for your review -" 
						+ " revise your comments".hyper(`/roles.view?client=${req.client}`)
						+ " as needed" );
		
				sql.query(
					"SELECT sum(Updates)*? AS Earnings FROM journal WHERE ?", [
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
						sql.query("SELECT Client FROM roles WHERE Power>=?", query.Power, function (err,ghawk) { // greater hawks
						sql.query("SELECT Client FROM roles WHERE Power<?", query.Power, function (err,lhawk) { // lesser hawks

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
							}, function (err) {
								console.log("Send="+err);
							});
						});
						});
				});
				break;
				
			case "itemized":
			default:
					
				Trace(sql.query(
					"SELECT concat(Dataset,'.',Field) AS Idx, "
					+ "group_concat(distinct linkrole(Hawk,Updates,Power)) AS Moderators "
					+ "FROM journal "
					+ "WHERE Updates "
					+ `GROUP BY ${pivot}`, 
					function (err,recs) {

					res( err || recs );
				}));

		}
	});

}

// Digital globe interface

FLEX.select.DigitalGlobe = function Select(req, res) {  
	var sql = req.sql, log = req.log, query = req.query;
}

// Hydra interface

FLEX.select.AlgorithmService = function Select(req, res) { 
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
				link: det.Name.tag("a",{href:"/swag.jade?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
		
	});

}

// Reserved for NCL and ESS service alerts
FLEX.select.NCL = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
}

FLEX.select.ESS = function Select(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
}

// Uploads/Stores file interface

FLEX.select.uploads = FLEX.select.stores = function Uploads(req, res) {
	var sql = req.sql, log = req.log, query = req.query,  body = req.body;
	var now = new Date();			
	var rtns = [], area = req.table;
	var path = `./public/${area}`;

	Trace(`DIR ${path}`);
		  
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
												href:"/files.jade?goto=ELT&option="+`${area}.${file}`
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
				"SELECT Area,Name,concat(Area,'.',Name) AS Ref, link(Name,concat('/',Area,'.',Name)) AS Link, astext(address) AS Address FROM files HAVING ?", 
				guardQuery(query,true), function (err,rtns) {
					res(err || rtns);
			});
	}
}

FLEX.update.stores = FLEX.insert.stores =
FLEX.update.uploads = FLEX.insert.uploads = function Uploads(req, res) {
	
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
	
	var 
		canvas = body.canvas || {objects:[]},
		attach = [],
		now = new Date(),
		image = body.image,
		area = req.table,
		files = image ? [{
			name: name, // + ( files.length ? "_"+files.length : ""), 
			size: image.length/8, 
			image: image
		}] : req.files || body.files || [];

	if (false)
		console.log({
		q: query,
		b: body,
		f: files,
		a: area
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

	if (FLEX.uploader)
	FLEX.uploader(files, area, function (file) {

		var geoloc = req.location || "POINT(0 0)";
		
		sql.query(	// this might be generating an extra geo=null record for some reason.  works thereafter.
			   "INSERT INTO files SET ?,address=geomfromtext(?) "
			+ "ON DUPLICATE KEY UPDATE ?,Revs=Revs+1,address=geomfromtext(?)", [{
				Client: req.client,
				Name: file.filename,
				Area: area,
				Added: new Date(),
				Classif: file.classif || "",
				Revs: 1,
				Tag: file.Tag || ""
			}, geoloc, { Client: req.client, Added:new Date() }, geoloc], function (err) {
				if (err)
					console.log(err);
			});

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

FLEX.execute.uploads = function Execute(req, res) {
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

// Execute interfaces

FLEX.execute.intake = function Execute(req, res) {
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
FLEX.update.engines = function Update(req, res) {
	var sql = req.sql, log = req.log, query = req.query, body = req.body;

	sql.query("UPDATE engines SET ? WHERE ?", [body,query])
	.on("end", function () {
		ENGINE.free(req,res);
	});
}*/

FLEX.execute.engines = function Execute(req, res) {
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
						ENGINE.read(req, res);
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
					
						if ( builtin = FLEX.execute[Name] ) 
							sql.query("INSERT into engines SET ?", {
								Code: builtin+"",
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

FLEX.execute.news = function Execute(req, res) {  
	
	function fixnews(req, res) {
		req.Message.split("\n").each( function (n,line) {
			res(line);
		});
	}
	
	var sql = req.sql, log = req.log, query = req.query;
	var mask = {
		today: new Date(),
		me: req.client
	};

	// export - email/socket those on to list anything marked new

	res(SUBMITTED);
	
	sql.query("SELECT * FROM news ", query)
	.on("result", function (news) {
		//sql.query("UPDATE news SET ? WHERE ?",[{New:false},{ID:news.ID}]);

		viaAgent(news, fixnews, req, function (msg, sql) {
			var 
				parts = msg.split("@"),
				subj = parts[0],
				rhs = parts[1] || "",
				to = rhs.substr(0,rhs.indexOf(" ")),
				body = rhs.substr(to.length);

			Trace(`NEWS ${to}`);

			switch (to) {
				case "conseq":

				case "wicwar":

				case "jira":
					break;

				case "":
					break;
					
				default:

					sendMail({
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
			//+ "link(concat(queues.sign0,queues.sign1,queues.sign2,queues.sign3,queues.sign4,queues.sign5,queues.sign6,queues.sign7),concat(?,intake.Name)) AS Waiting, "
			+ "link(states.Name,'/parms.jade') AS State "
			+ "FROM intake "
			+ "LEFT JOIN queues ON (queues.Client=? and queues.State=intake.TRL and queues.Class='TRL' and queues.Job=intake.Name) "
			+ "LEFT JOIN states ON (states.Class='TRL' and states.State=intake.TRL) "
			+ "WHERE intake.?", ["/intake.jade?name=","/queue.jade?name=",client,{ Name:name }] ) 
		.on("error", function (err) {
			Trace(err);
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

FLEX.execute.milestones = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var map = {SeqNum:1,Num:1,Hours:1,Complete:1,Task:1};

	for (var n=0;n<=10;n++) map["W"+n] = 1;

	sql.query("DELETE FROM openv.milestones");
	FLEX.RDR.xlsx(sql,"milestones.xlsx","stores",function (rec) {
		for (var n in map) map[n] = rec[n] || "";
		
		sql.query("INSERT INTO openv.milestones SET ?",map, function (err) {
			if (err) Trace(err);
		});
	});

	res(SUBMITTED);
}

FLEX.execute.sockets = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	if (FLEX.emitter)
		sql.query("SELECT * FROM sockets WHERE length(Message)")
		.on("result", function (sock) {
			FLEX.emitter("alert", {msg: sock.Message, to: sock.Client, from: req.client});
			sql.query("UPDATE sockets SET ? WHERE ?",[{Message:""},{ID:sock.ID}]);
		});
		
	res(SUBMITTED);
}

FLEX.execute.tests = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	res(SUBMITTED);
	
	sql.query(
		"SELECT *,tests.ID AS testID,tests.Channel as chanName,detectors.Name as detName FROM tests "
		+ " LEFT JOIN detectors ON detectors.Channel=tests.Channel AND Enabled HAVING ?",
		{testID:query.ID}
	)
	.on("result", function (test) {
		
console.log(req.profile);
		
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
				link: test.detName.tag("a",{href:"/swag.jade?goto=Detectors"}),
				qos: req.profile.QoS,
				priority: 1
			}
		});
	});

}

FLEX.execute.collects = function Execute(req, res) {
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

FLEX.execute.detectors = function Execute(req, res) {
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
	
	Trace("PRIME DETECTORS");
	
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

FLEX.execute.events = function Execute(req, res) {
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

FLEX.execute.issues = 
FLEX.execute.aspreqts = 
FLEX.execute.ispreqts = function Execute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

FLEX.execute.parms = function Execute(req, res) { 
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
	
	var allparms = {}, ntables = 0;
	sql.indexTables("app1", function (tname) {
		
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
	
	/*if (true)
	sql.query("SHOW TABLES FROM app1 WHERE Tables_in_app1 NOT LIKE '\\_%'", function (err,tables) {	// sync parms table with DB
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
						
						//console.log([pname,ptype]);
						
						var parm = {Tables:tablelist, Parm:pname, Type:ptype};

						sql.query("INSERT INTO parms SET ? ON DUPLICATE KEY UPDATE ?",[parm,parm]);
					});
			});

		});
	});*/
	
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

FLEX.execute.attrs = function Execute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	var created = [];
		
	res(SUBMITTED);

	sql.query("SELECT * FROM openv.attrs")
	.on("result", function (dsattr) {		
		sql.query("CREATE TABLE ?? (ID float unique auto_increment)", dsattr.Table);
	});
}

FLEX.execute.lookups = function Execute(req, res) {
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

FLEX.execute.searches = function Execute(req, res) {
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
					
FLEX.execute.chips = function Execute(req, res) {   
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

FLEX.execute.swaps = function Execute(req, res) {   
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

		Trace("INSTALLING "+package);

		// submit swaps that have not already been submitted 
		
		sql.query("SELECT * FROM swaps WHERE Enabled AND NOT Submitted")
		.on("result", function (swap) {
			sql.query("UPDATE swaps SET ? WHERE ?",[{Submitted: new Date()}, {ID:swap.ID}]);
			
			sendMail({
				to:  site.POC,
				subject: "SWAP "+package,
				html: "Please find attached SWAP.  Delivery of  "+swap.Product+" is conditional on NGA/OCIO acceptance of this deviation request. ",
				alternatives: [{
					contentType: 'text/html; charset="ISO-59-1"',
					contents: ""
				}]
			}, function (err,info) {
				if (err) Trace(err);
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
					Trace("UNPACKED "+package);
				});
			else {  // crawl site with phantomjs to pull latest source
			}

		});
		});
	});
	
}

FLEX.execute.hawks = function Execute(req, res) {
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

	var sql = req.sql, query = req.query;
	
	res(SUBMITTED);
	
	sql.hawkJobs(req.client,FLEX.site.masterURL);
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
*/

// Detectors 

FLEX.execute.detectors = function Execute(req, res) { 
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
		
		Trace("PROOFING ",[posProofs.length,negProofs.length]);

		if (posProofs.length && negProofs.length) {	// must have some proofs to execute job
			
			var	posDirty = posProofs.sum("dirty"),
				negDirty = negProofs.sum("dirty"),
				totDirty = posDirty + negDirty,
				totProofs = posProofs.length + negProofs.length,
				dirtyness = totDirty / totProofs;

			Trace('DIRTY', [dirtyness,job.MaxDirty,posDirty,negDirty,posProofs.length,negProofs.length]);

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
					
					det.link = det.Name.tag("a",{href:"/swag.jade?goto=Detectors"}) + " " + det.posLimit + " pos " + det.negLimit + " neg";
					det.name = det.Name;
					det.client = log.client;
					det.work = det.posCount + det.negCount;

					Trace(`TRAIN ${det.Name} v${ver}`);
				
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
						
					Trace((det.Execute||"").toUpperCase()+" "+det.name);
					
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
		
	Trace("STAT GIT");
	
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
	sql.query("SELECT * FROM features WHERE NOT ad")
	.on("result", function (feature) {
		NEWSFEED.addItem({
			title:          feature.feature,
			link:           `${FLEX.paths.HOST}/feed.jade`,
			description:    JSON.stringify(feature),
			author: [{
				name:   FLEX.site.title,
				email:  FLEX.mailer.SOURCE,
				link:   FLEX.paths.HOST
			}],
			/*contributor: [{
				name:   FLEX.TITLE,
				email:  site.email.SOURCE,
				link:   FLEX.paths.HOST
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
	Trace(err);
	process.exit(1);
}

function openIMAP(cb) {
	FLEX.mailer.RX.TRAN.connect(function(err) {  // login cb
		if (err) killIMAP(err);
		FLEX.mailer.RX.TRAN.openBox('INBOX', true, cb);
	});
}

function sendMail(opts, cb) {
	
	Trace(`MAIL ${opts.to} RE ${opts.subject}`);
		
	if (x = FLEX.mailer)
		if (x = x.TX.TRAN) 
			if (opts.to) {
				opts.from = "noreply@nga.ic.gov";
				x.sendMail(opts,cb);
			}
}

/**
 * @method flattenCatalog
 * 
 * Flatten entire database for searching the catalog
 * */
function flattenCatalog(flags, catalog, limits, cb) {
	
	function flatten( sql, rtns, depth, order, catalog, limits, cb) {
		var table = order[depth];
		
		if (table) {
			var match = catalog[table];
			var filter = cb.filter(match);
			
			var quality = " using "+ (filter ? filter : "open")  + " search limit " + limits.records;
			
			Trace("CATALOG ("+table+quality+") HAS "+rtns.length);
		
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
 * Job queue interface
 * 
 * select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
 * execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
 * update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
 * delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
 * insert(job,cb): add job and route to callback cb(job) when executed.
 * */

FLEX.queues = {};
	
/*
* @method selectJob
* @param {Object} req job query
* @param {Function} cb callback(rec) when job departs
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
		Trace(err);
	})
	.on("result", function (rec) {
		cb(rec);
	});	
}

/*
* @method updateJob
* @param {Object} req job query
* @param {Function} cb callback(sql,job) when job departs
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
* @method deleteJob
* @param {Object} req job query
* @param {Function} cb callback(sql,job) when job departs
* >>> Not used but needs work
*/
function deleteJob(req, cb) { 
	
	var sql = this;
	sql.selectJob(req, function (job) {
		
		cb(sql,job, function (ack) {
			sql.query("UPDATE queues SET ?,Age=(now()-Arrived)*1e-3 WHERE ?", [{
				Departed: new Date(),
				Util: util(),
				Notes:"stopped"}, 
				{Name:job.name,Client:job.client,Class:job.class}]);

			delete FLEX.queues[job.qos].batch[job.priority];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

/*
* @method insertJob
* @param {Object} job arriving job
* @param {Function} cb callback(sql,job) when job departs
*
 * Adds job to requested job.qos, job.priority queue and updates the
 * queuing log keyed by job (name,client,class).  A departing job 
 * execute the supplied callback cb(sql,job) on a new sql thread, or
 * spawns the job if job.cmd provided.
 */
function insertJob(job, cb) { 
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
			
		var queue = FLEX.queues[job.qos];	// get job's qos queue
		
		if (!queue)
			Trace("MAKE QUEUE", queue = FLEX.queues[job.qos] = {
				timer: 0,
				batch: {},
				rate: 2e3*(10-job.qos)
			} );
			
		if (queue.rate > 0) { 				// regulated job
			var batch = queue.batch[job.priority]; 		// get job's priority batch
			if (!batch) 
				Trace("MAKE BATCH", batch = queue.batch[job.priority] = [] );
			
			batch.push( Copy(job, {cb:cb}) );
			
			if (!queue.timer) 		// restart idle queue
				queue.timer = setInterval(function (queue) {
					
					var job = null;
					for (var priority in queue.batch) {  // index thru all priority batches
						var batch = queue.batch[priority];
						
						job = batch.pop(); 			// last-in first-out
						
						if (job) {
//console.log("job depth="+batch.length+" job="+[job.name,job.qos]);

							if (job.cmd)	// spawn job and return its pid
								job.pid = CP.exec(
										job.cmd, 
										  {cwd: "./public/dets", env:process.env}, 
										  function (err,stdout,stderr) {

							job.err = err || stderr || stdout;

							if (job.cb)
								FLEX.thread( function (sql) {
									job.cb( sql, job );
								});
						});
							
							else  			// execute job cb on a new sql thread
							if (job.cb) 
								FLEX.thread( function (sql) {
									job.cb(sql, job);
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
		jobID = {Name:job.name, Client:job.client, Class:job.class};
	
	var
		regulated = regulate(job, function (sql,job) {
				
			cb(sql,job);
			
			sql.query(
				"UPDATE app1.queues SET ?,Age=(now()-Arrived)*1e-3,Done=Done+1,State=Done/Work*100 WHERE least(?)", [
				{Util: util()}, 
				jobID 
			]);
			
			sql.query(
				"UPDATE app1.queues SET Departed=now(), Age=(now(),Arrived)*1e-3,Notes='finished' WHERE least(?,Done=Work)", 
				jobID);

		});

	if (regulated)
			sql.query("INSERT INTO app1.queues SET ?", {
				Client	: job.client,
				Class	: job.class,
				State	: 0,
				Arrived	: new Date(),
				Departed: null,
				Mark	: 0,
				Name	: job.name,
				Age	: 0,
				Classif : "",
				Util	: util(),
				Priority: job.priority,
				Notes	: "running",
				QoS		: job.qos,
				Work 	: 1
			}, function (err) {
				
				if (err)
					sql.query(
						"UPDATE app1.queues SET ?,Age=(now()-Arrived)*1e-3,Work=Work+1,State=Done/Work*100 WHERE least(?)", [
							{Util:util()},
							jobID
						]);
					
			});

	else
		cb(sql, job);
}
	
function executeJob(req, exe) {
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

// Database CRUDE interface

function runQuery(req, res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		query = req.query,
		body = Copy(query,req.body);

	delete body.ID;

	sql.context({ds: {
			table:	req.table,
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

//  job monitors 

function hawkJobs (client, url)  {
	var sql = this;

	function hawk(rule) {
		FLEX.thread(function (sql) {

			sql.query("UPDATE app1.hawks SET Pulse=Pulse+1 WHERE ?", {ID:rule.ID});

			if (rule.Action.charAt(0) == "/" && FLEX.execute)
				FLEX.fetcher(url + rule.Action, function (ack) {
					console.log(ack);
				});
			
			else
				switch (rule.Action.toLowerCase()) {
					case "scrape":
					case "execute":

						FLEX.CRUDE({
							query: {},
							body: {},
							param: function () { return ""; }
							/*connection: {
								listeners: function () {return "";},
								on: function () {return this;},
								setTimeout: function () {}
							}*/
						}, {
							send: function (rtn) {
								console.log(rtn);
							} 
						}, rule.Table, "execute");

						break;

					case "stop":
					case "halt":
					case "delete":
					case "kill":

						sql.jobs().delete(rule.Condition, function (job) {
							sql.query("UPDATE app1.hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
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
									to:  job.Client,
									subject: FLEX.site.title + " job status notice",
									html: "Please "+"clear your job flag".tag("a",{href:"/rule.jade"})+" to keep your job running.",
									alternatives: [{
										contentType: 'text/html; charset="ISO-59-1"',
										contents: ""
									}]
								}, function (err,info) {
									if (err) Trace(err);
								});

							});
						});

						break;

					case "promote":
					case "improve":

						sql.jobs().update(rule.Condition, +1, 0, function (job) {
							sql.query("UPDATE app1.hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
						});

						break;

					case "demote":
					case "reduce":

						sql.jobs().update(rule.Condition, -1, 0, function (job) {
							sql.query("UPDATE app1.hawks SET Changed=Changed+1 WHERE ?", {ID:rule.ID});
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

	FLEX.timers.each( function (n,id) { 	// kill existing hawks
		clearInterval(id);
	});

	FLEX.timers = [];

	//sql.query("DELETE FROM queues"); 		// flush job queues
	
	sql.query("SELECT * FROM app1.config WHERE Hawks")             // get hawk config options
	.on("result", function (config) {

		sql.query(
			"SELECT * FROM app1.hawks WHERE least(?) AND Faults<?  AND `Condition` IS NOT NULL", [
			{Enabled:1, Name:config.SetPoint}, 
			config.MaxFaults
		])
		.on("result", function (rule) {         // create a hawk
			
			Trace(`HAWKING if[${rule.Condition}]then[${rule.Action}]  EVERY ${rule.Period} mins`);

			if (rule.Period)                     // hawk is periodic
				FLEX.timers.push(
					setInterval( hawk, rule.Period*60*1000, rule )
				);
			else                                 // one-time hawk
				hawk(rule);
		});
	});
}

FLEX.execute.gaussmix = function (req, res) {
	
	var 
		sql = req.sql,
		exp = Math.exp, log = Math.log, sqrt = Math.sqrt, floor = Math.floor, rand = Math.random;

	function randint() {
		return floor(rand() * 10);
	}
	
	function gaussmix (req,res) {
		var run = {};

		try {
			var 
				mix = JSON.parse(req.Mix) || [];  // gauss mixing parameters
			
			if (mix.constructor == Object) {
				var K = mix.K, mix = [];
				
				for (var k=0; k<K; k++) {
					var xx = 0.9, yy = 0.7, xy = yx = 0.4;
					mix.push({
						mu: [randint(), randint()],
						sigma: [[xx,xy],[yx,yy]]
					});
				}
				console.log(mix);
			}
			
			var
				mvd = [], 	// multivariate distribution parms
				ooW = [], // wiener/oo process look ahead
				mixes = mix.length,	// number of mixes
				mode = mixes ? parseFloat(mix[0].theta) ? "oo" : mix[0].theta || "gm" : "na",  // mix mode
				
				mix0 = mix[0] || {},  // wiener/oo parms (using mix[0] now)
				mu = mix0.mu,	// mean 
				sigma = mix0.sigma,  // covariance
				theta = mix0.theta,  	// oo time lag
				x0 = mix0.x0, 		// oo initial pos
				a = {  // process fixed parms
					wi: 0,
					gm: 0,
					br: sigma*sigma / 2,
					oo: sigma / sqrt(2*theta)
				},

				sampler = {  // sampling method during RAN jumps
					na: function (n,fr,to,h,x) {
					},
					
					wi: function (n,fr,to,h,x) {  // wiener
						var 
							t = RAN.s, 
							Wt = RAN.W[0], 
							xt = mu + sigma * Wt;
						
						x.push( xt );
					},
					
					oo: function (n,fr,to,h,x) {  // ornstein-uhlenbeck
						var 
							t = RAN.s, 
							Et = exp(-theta*t),
							Et2 = exp(2*theta*t),
							Wt = ooW[floor(Et2 - 1)] || 0,
							xt = x0 ? x0 * Et + mu*(1-Et) + a.oo * Et * Wt : mu + a.oo * Et * Wt;
						
						ooW.push( W[0] );
						x.push( xt );
					},
						
					br: function (n,fr,to,h,x) { // geometric brownian
						var 
							t = RAN.s, 
							Wt = RAN.W[0],
							xt = exp( (mu-a.br)*t + sigma*Wt );
						
						x.push( xt );
					},
						
					gm: function (n,fr,to,h,x) {  // mixed gaussian
						var xt = mvd[to].sample();
						
						x.push( xt );
					}
				};

			if (mode == "gm")
				for (var k=0; k<mixes; k++)
					mvd.push( RAN.MVN( mix[k].mu, mix[k].sigma ) );

			RAN.config({
				N: req.Ensemble,
				wiener: req.Wiener,
				A: JSON.parse(req.JumpRates || "[]"),
				sym: JSON.parse(req.Symbols || "null"),
				nyquist: req.Nyquist,
				x: mixes ? [] : null,  // jump obs
				y: [],  // step obs
				bins: 50,
				
				// debugging
				//A: [[0,1],[1,0]], //[[0,1,2],[3,0,4],[5,6,0]],
				//sym: [-1,1],
				//nyquist: 10,

				on: {
					jump: sampler[mode],
					
					step: function (y) {
						var  
							t = RAN.t, n = t / RAN.Tc, N = RAN.N, 
							cnt = N-RAN.E[0], lambda = RAN.G[0]/t, lambda0 = N/RAN.dt;

						y.push( [ n, RAN.corr(), exp(-n), cnt, lambda / lambda0 ].concat(RAN.W) );
					}
				}
			});
		}
		catch (err) {
			console.log (err);
		}

		var info = {
			cellCount: mixes,
			sampleMode: mode,
			randomWalks: RAN.wiener,
			ensembleSize: RAN.N,
			coherenceTime: RAN.Tc,
			coherenceIntervals: req.Steps,
			ensembleActivity: RAN.p,
			ensembleLoad: RAN.lambda,
			sampleTime: RAN.dt,
			reqName: req.Name			
		};
		
		console.log(info);

		RAN.run(req.Steps * RAN.Tc/RAN.dt, function (x,y,stats) {

			function dist(a,b) { 
				var d = [ a[0]-b[0], a[1]-b[1] ];
				return sqrt( d[0]*d[0] + d[1]*d[1] );
			}

			Array.prototype.nearestOf = function (metric) {
				var imin = 0, emin = 1e99;

				this.each( function (i,req) {
					var e = metric( req );
					if (  e < emin) { imin = i; emin = e; }
				});
				return {idx: imin, err: emin};
			}

			Copy({
				processSteps: RAN.steps,
				processSamples: RAN.jumps,
				processStats: JSON.stringify({
					jumpRates: RAN.A,
					cumTxPr: RAN.P,
					jumpCounts: RAN.T,
					holdTimes: RAN.R,
					symbols: RAN.sym,
					initPr: RAN.pi						
				})
			}, info);

			if (x) {  // generate, grade, sort and store gauss mixing mle results
				var 
					gmms = RAN.MLE(x, mixes);

				gmms.each( function (k,gmm) {
					gmm.find = mix.nearestOf(function (req) {
						return dist( req.mu, gmm.mu );
					});
				});

				gmms.sort( function (a,b) {
					return a.find.idx < b.find.idx ? 1 : -1;
				});

				gmms.each(function (n,gmm) {
					Copy({
						cellIndex: gmm.find.idx,
						cellType: "mle",
						cellError: gmm.find.err * 100,
						cellParms: JSON.stringify({
							mu: gmm.mu,
							sigma: gmm.sigma
						})
					}, info);
						
					sql.query("REPLACE INTO gaussruns SET ?", info, function (err) {
						console.log(err || "cell saved");
					});
				});

				mix.each(function (n,mix) {					
					Copy({
						cellIndex: n,
						cellType: "true",
						cellError: 0,
						cellParms: JSON.stringify({
							mu: mix.mu,
							sigma: mix.sigma
						})
					}, info);
						
					sql.query("REPLACE INTO gaussruns SET ?", info, function (err) {
						console.log(err || "cell saved");
					});
				});

				res( Copy({
					steps: y,
					stats: stats,
					info: info
				}, run) );
				
console.log(JSON.stringify(gmms)); 
			}
			
			else
				res( Copy({
					jumps: x,
					steps: y,
					stats: stats,
					info: info
				}, run) );
		});
	}
	
	res("submitted");
	
	sql.query("SELECT * FROM app1.gaussmix WHERE least(?,1)", req.query)
	.on("result", function (mix) {
		
//console.log(mix);
		viaAgent(mix, gaussmix, req, function (rtn, sql) {
			
			sql.query("UPDATE app1.gaussmix SET ? WHERE ?", [{
				Result: JSON.stringify(rtn)}, {
				Name: mixreq.Name
			}], function (err) {
				console.log(err || "GAUSSMIX " + mixreq.Name);
			});
			
		});

	});
}

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

function viaAgent(args, job, req, res) {
	var
		fetch = FLEX.fetcher,
		sql = req.sql,
		query = req.query,
		jobname = "totem."+ req.client + "." + job.name + "." + query.ID;
	
	if (agent = query.agent) 
		fetch(agent+"&push="+jobname+"&args="+JSON.stringify(args), function (jobid) {
			
			if (jobid)
				if (jobid.constructor == Error)
					Trace(jobid);

				else {
					Trace("AGENT FORKED "+jobname);
		
					sql.query("INSERT INTO queues SET ?", {
						class: agent,
						client: req.client,
						qos: 0,
						priority: 0,
						name: job.name
					});

					if ( poll = parseInt(query.poll) )
						var timer = setInterval(function (req) {

							Trace("AGENT POLLING "+jobid);

							fetch(req.agent+"?pull="+jobid, function (rtn) {

								if (rtn) 
									if (rtn.constructor == Error) {
										Trace("AGENT FAILED");
										clearInterval(timer);
										req.cb( rtn, sql );
									}

									else
										FLEX.thread( function (sql) {
											Trace("AGENT RETURNED "+jobid);
											sql.query("DELETE FROM queues WHERE ?", {Name: req.job.name});								
											req.cb( rtn, sql );
										});

							});

						}, poll*1000, Copy({
								job: job,
								cb: res}, req));
				}
			
			else
				Trace("AGENT REJECTED "+jobname);
	
		});

	else 
		job(args, function (rtn) {
			res(rtn,sql);
		});
}
			
// UNCLASSIFIED
