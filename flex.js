// UNCLASSIFIED

/*
@class FLEX
Provides CRUD interface to virtual datasets as documented in README.md.

@requires vm
@requires http
@requires url
@requires cluster
@requires fs
@requires child-process
@requires os

@requires enum
@requires atomic
@requires reader
@requires man

@requires uglify-js
@requires html-minifier
@requires pdffiller
@requires nodemailer
@requires nodemailer-smtp-transport
@requires imap
@requires feed
@requires feed-read
@requires jshint
@requires prettydiff
@requires neo4j
*/

var 
	// globals
	ENV = process.env, 					// external variables
	SUBMITTED = "submitted",

	// totem bindings required before others due to dependent module issues
	//READ = require("reader")(),		// NLP partial config now to avoid prototype collisions
	
	VM = require('vm'), 				// V8 JS compiler
	//STR = require("stream"), 	// pipe-able streams
	CLUS = require('cluster'),		// Support for multiple cores	
	HTTP = require('http'),				// HTTP interface
	//NET = require('net'), 				// Network interface
	URL = require('url'), 				// Network interface
	FS = require('fs'),					// Need filesystem for crawling directory
	CP = require('child_process'), 		// Child process threads
	OS = require('os'),					// OS utilitites

	// 3rd party bindings
	//PDF = require('pdffiller'), 		// pdf form processing
	MAIL = require('nodemailer'),		// MAIL mail sender
	SMTP = require('nodemailer-smtp-transport'),
	IMAP = require('imap'),				// IMAP mail receiver
	FEED = require('feed'),				// RSS / ATOM news feeder
	//RSS = require('feed-read'), 		// RSS / ATOM news reader

	// totem bindings
	$ = require("man"), 			// matrix minipulaor
	ATOM = require("atomic");		// tauif simulation engines
	//RAN = require("randpr"), 		// random process

function Trace(msg,req,fwd) {
	"flex".trace(msg,req,fwd);
}

const { Copy,Each,Log,isObject,isString,isFunction,Stream,isError,isEmpty } = require("enum");

var FLEX = module.exports = {
	config: opts => {
		/**
		@method config
		Configure module with spcified options, publish plugins under TYPE/NOTEBOOK.js, establish
		email sender and news feeder.   
		*/
		if (opts) Copy(opts,FLEX,".");

		var
			site = FLEX.site;

		if (email = FLEX.mailer.TX) {
			//Log("emailset", site.emailhost, "pocs", site.pocs);

			var parts = (site.emailhost||"").split(":");
			email.HOST = parts[0];
			email.PORT = parseInt(parts[1]);

			var parts = (site.emailuser||"").split(":");
			email.USER = parts[0];
			email.PASS = parts[1];

			if (email.PORT) {  		// Establish server's email transport
				Trace(`MAILHOST ${email.HOST} ON port-${email.PORT}`);

				email.TRAN = email.USER
					? MAIL.createTransport({ //"SMTP",{
						host: email.HOST,
						port: email.PORT,
						auth: {
							user: email.USER,
							pass: email.PASS
						}
					})
					: MAIL.createTransport({ //"SMTP",{
						host: email.HOST,
						port: email.PORT
					});
			}

			else
				email.TRAN = {
					sendMail: function (opts, cb) {
						Log(opts);
						CP.exec(`echo -e "${opts.body||'FYI'}\n" | mail -r "${opts.from}" -s "${opts.subject}" ${opts.to}`, err => {
							cb( err );
							//Trace("MAIL "+ (err || opts.to) );
						});
					}
				};
		}
		
		if ( sqlThread = FLEX.sqlThread)
			sqlThread( sql => {	
				//READ( null, sql );

				if (CLUS.isMaster && 0)   					
					FLEX.publishPlugins( sql );

				if (false)
					sql.query("SELECT Name FROM app.engines WHERE Enabled")
					.on("result", function (eng) { // prime the associated dataset
						Trace("PRIME plugin-"+eng.Name, sql);
						sql.query(
							"CREATE TABLE app.?? (ID float unique auto_increment, Name varchar(32), "
							+ "Description mediumtext)",
							eng.Name );
					});
			});

		if (CLUS.isMaster) {
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

			if (email = FLEX.mailer.RX) {
				if (email.PORT)
					email.TRAN = new IMAP({
						  user: email.USER,
						  password: email.PASS,
						  host: email.HOST,
						  port: email.PORT,
						  secure: true,
						  //debug: err => { console.warn(ME+">"+err); } ,
						  connTimeout: 10000
						});

				else
					email = {};

				if (email.TRAN)					// Establish server's email inbox			
					openIMAP( function(err, mailbox) {
						if (err) killIMAP(err);
						email.TRAN.search([ 'UNSEEN', ['SINCE', 'May 20, 2012'] ], function(err, results) {

							if (err) killIMAP(err);

							email.TRAN.fetch(results, { 
								headers: ['from', 'to', 'subject', 'date'],
								cb: function(fetch) {
									fetch.on('message', function(msg) {
										Log(ME+'Saw message no. ' + msg.seqno);
										msg.on('headers', function(hdrs) {
											Log(ME+'Headers for no. ' + msg.seqno + ': ' + showIMAP(hdrs));
										});
										msg.on('end', function() {
											Log(ME+'Finished message no. ' + msg.seqno);
										});
									});
								}
							  }, function(err) {
								if (err) throw err;
								Log(ME+'Done fetching all messages!');
								email.TRAN.logout();
							});
					  });
					});
			}
		}
	},
	
	mailer : {						// Email parameters
		TRACE 	: true,	
		SOURCE: "tbd",
		TX: {},
		RX: {}
	},

	//licenseOnDownload: false,
	//licenseOnRestart: false,

	publishPlugins: function ( sql ) { //< publish all plugins
		var 
			types = FLEX.publish;

		sql.query( "SELECT * FROM app.publisher WHERE Enabled")
		.on("result", pub => {
			Trace("PUBLISH PATH "+pub.Path);
			types.forEach( type => { 	// get plugin types to publish	
				FLEX.getIndex( "./"+pub.Path+"/"+type, files => {	// get plugin names to publish
					files.forEach( file => {
						var
							product = file,
							parts = product.split("."),
							filename = parts[0] || "NoName",
							filetype = parts[1] || "js";
							//filepath = "./"+pub.Path+"/"+type+"/"+filename;

						switch (filetype) {
							case "js":
								//FLEX.publishPlugin(sql, filepath, filename, type, FLEX.licenseOnRestart);
								FLEX.probeSite( `/${filename}.pub`, info => Trace( "PUBLISH "+info) );
								break;

							case "jade":
								/*
								FS.readFile( path + file, "utf8", (err,code) => {
									if (!err)
										sql.query( 
											"INSERT INTO app.engines SET ? ON DUPLICATE KEY UPDATE Code=?", [{
												Name: filename,
												Code: code,
												Type: filetype,
												Enabled: 1
											}, code
										]);	
								});*/
								break;

						}
					});	
				});
			});
		});
	},

	sendMail: sendMail,

	errors: {
		//noLicense: new Error("Failed to prepare a product license"),
		badRequest: new Error("bad/missing request parameter(s)"),
		noBody: new Error("no body keys"),
		badBaseline: new Error("baseline could not reset change journal"),
		disableEngine: new Error("requested engine must be disabled to prime"),
		noEngine: new Error("requested engine does not exist"),
		missingEngine: new Error("missing engine query"),
		protectedQueue: new Error("action not allowed on this job queues"),
		noCase: new Error("plugin case not found"),
		badAgent: new Error("agent failed"),
		badDS: new Error("dataset could not be modified"),
		badLogin: new Error("invalid login name/password"),
		failedLogin: new Error("login failed - admin notified"),
		noUploader: new Error("file uplaoder not available")
	},

	attrs: {  // static table attributes (e.g. geo:"fieldname" to geojson a field)
	},

	site: { // reserved for site context
	},

	diag: { // configured for system health info
	},

	publish: [ "js", "py", "me", "m", "jade", "R" ],

	paths: {
		chips: "./chips/",
		status: "./shares/status.xlsx",
		logins: "./shares/logins",
		newsread: "http://craphound.com:80/?feed=rss2",
		aoiread: "http://omar.ilabs.ic.gov:80/tbd",
		host: ""
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

	// CRUDE interface
	select: {}, 
	delete: {}, 
	update: {}, 
	insert: {}, 
	execute: {}, 

	probeSite: (url,opt) => { throw new Error("flex never configured probeSite method"); } ,  //< data probeSite
	sqlThread: () => { throw new Error("flex never configured sqlThread method"); },  //< sql threader

	getContext: function ( sql, host, query, cb ) {  //< callback cb(ctx) with primed plugin context or cb(null) if error

		/*
		function config(js, ctx) {
			try {
				VM.runInContext( js, VM.createContext(ctx)  );	
			}
			catch (err) {
				Log("config ctx", err);
			}
		} */

		sql.forFirst("", "SELECT * FROM app.?? WHERE least(?,1) LIMIT 1", [host,query], ctx => {
			if (ctx) {
				ctx.Host = host;
				// if ( ctx.Config ) config(ctx.Config, ctx);

				sql.getJsons( `app.${host}`, keys => {
					keys.forEach( key => {
						if ( key.startsWith("Save") ) 
							ctx[key] = null;

						else
						if ( val = ctx[key] ) 
							ctx[key] = val.parseJSON( );
					});
					cb( ctx );
				});
			}

			else
				cb( null );	
		});

	},

	runPlugin: function runPlugin(req, res) {  //< callback res(ctx) with resulting ctx or cb(null) if error
	/**
	@method runPlugin
	Run a dataset-engine plugin named X = req.table using parameters Q = req.query
	or (if Q.id or Q.name specified) dataset X parameters derived from the matched  
	dataset (with json fields automatically parsed). On running the plugin's engine X, this 
	method then responds on res(results).   If Q.Save is present, the engine's results are
	also saved to the plugins dataset.  If Q.Pipe is present, then responds with res(Q.Pipe), 
	thus allowing the caller to place the request in its job queues.  Otherwise, if Q.Pipe 
	vacant, then responds with res(results).  If a Q.agent is present, then the plugin is 
	out-sourced to the requested agent, which is periodically polled for its results, then
	responds with res(results).  Related comments in FLEX.config.
	*/

		//Log("run req",req);
		const { sql, table, query } = req;
		
		FLEX.getContext( sql, table, {Name: query.Name}, ctx => {
			function logger () {	// notebook logger
				var args = [], msg = "";
				for (var key in arguments) 
					if ( key == "0" ) 
						msg = arguments[0]+"";
					else
						args.push( arguments[key] );

				"pipe".trace( msg+JSON.stringify(args), req, console.log );
			}
					
			if (ctx) {
				ctx.Trace = logger;

				req.query = ctx;
				
				if ( ctx.Pipe )  // let host chipping-regulating service run this engine
					res( ctx );

				else
				if ( viaAgent = FLEX.viaAgent )  // allow out-sourcing to agents if installed
					viaAgent(req, res);

				else   // in-source the plugin and save returned results
					//FLEX.runEngine(req, res);
					ATOM.select(req, res);
			}

			else
				res( null );
		});

	},

	viaAgent: function( req, res ) {  //< out-source a plugin with callback res(ctx) or res(null) if errror
	/**
	@method viaAgent
	Out-source the plugin to an agent = req.query.agent, if specified; otherwise, in-source the
	plugin. Callsback res(results, sql) with a possibly new sql thread.
	**/

		var
			probeSite = FLEX.probeSite,
			sql = req.sql,
			query = req.query;

		//Log({viaagent: query});

		if (agent = query.agent)   // out-source request
			probeSite(agent.tag( "?", query), jobid => {

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
						var timer = setInterval( req => { 

							Trace("POLLING AGENT FOR job"+jobid);

							probeSite(req.agent.tag("?",{pull:jobid}), ctx => {

								if ( ctx = ctx.parseJSON() )
									FLEX.sqlThread( sql => {
										Trace("FREEING AGENT FOR job-"+jobid, sql);
										sql.query("DELETE FROM app.queues WHERE ?", {Name: plugin.name});								
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

		else   // in-source request
			ATOM.select(req, res);
	}
};

var
	SELECT = FLEX.select,
	UPDATE = FLEX.update,
	DELETE = FLEX.delete,
	INSERT = FLEX.insert,
	EXECUTE = FLEX.execute;

/*
// Job queue CRUDE interface
SELECT.jobs = function Xselect(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	// route valid jobs matching sql-where clause to its assigned callback res(job).
	
	Trace( sql.query(
		"SELECT *, datediff(now(),Arrived) AS Age,openv.profiles.* FROM queues LEFT JOIN openv.profiles ON queues.Client=openv.profiles.Client WHERE NOT Hold AND Departed IS NULL AND NOT openv.profiles.Banned AND least(?,1) ORDER BY queues.QoS,queues.Priority", 
		guardQuery(query,true), function (err, recs) {
			res( err || recs );
	}));
	
}

UPDATE.jobs = function Update(req, res) { 
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
		
DELETE.jobs = function Delete(req, res) { 
	var protect = true;
	var sql = req.sql, log = req.log, query = req.query, body = req.body;
		
	if (protect)
		res( FLEX.errors.protectedQueue );
	else
		sql.query("DELETE FROM queues WHERE ?",query, function (err,info) {
			res( err || info );
		});
}

INSERT.jobs = function Insert(req, res) { 
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
SELECT.sql = function Xselect(req, res) {
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

INSERT.sql = function Insert(req, res) {
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

DELETE.sql = function Delete(req, res) {
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

UPDATE.sql = function Update(req, res) {
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

EXECUTE.baseline = function Xexecute(req,res) {  // baseline changes

	var	
		sql = req.sql, 
		query = req.query;
	
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
	
	var
		login = `-u${ENV.MYSQL_USER} -p${ENV.MYSQL_PASS}`,
		ex = {
			group: `mysqldump ${login} app >admins/db/app.sql`,
			openv: `mysqldump ${login} openv >admins/db/openv.sql`,
			clear: `mysql ${login} -e "drop database baseline"`,
			prime: `mysql ${login} -e "create database baseline"`,
			rebase: `mysql ${login} baseline<admins/db/openv.sql`,
			commit: `git commit -am "${req.client} baseline"`
		};

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

SELECT.baseline = function Xselect(req, res) {

	var 
		query = req.query,
		sql = req.sql;
	
	CP.exec(
		'git log --reverse --pretty=format:"%h||%an||%ce||%ad||%s" > gitlog',
		function (err,log) {

		var recs = [];

		if (err)
			res(recs);
		
		else
			FS.readFile("gitlog", "utf-8", function (err,logs) {
				logs.split("\n").forEach( (log,id) => {

					var	parts = log.split("||");

					recs.push({	
						ID: id,
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

EXECUTE.catalog = function Xexecute(req, res) {
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
		
		recs.forEach( rec => {
			sql.query("INSERT INTO catalog SET ?",rec);
		});

	});
	
}

SELECT.tasks = function Xselect(req,res) {
	var sql = req.sql, query = req.query;
	var tasks = [];
	
	sql.query("SELECT Task FROM openv.milestones GROUP BY Task", function (err,miles) {
		
		if ( !err )
			miles.forEach( mile => {
				tasks.push({
					ID: tasks.length,
					Name: mile.Task.tag("a",{href: "/task.view?options="+mile.Task})
				});
			});
		
		res( tasks );		
		
	});
}

// Getters

SELECT.activity = function Xselect(req, res) {
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
			
			attrs.forEach( dsattr => {
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
			
			acts.forEach( act => {
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
			
			res( recs.sample( function (rec) { 
				return rec.ID; 
			}) );
	});
	});
}

SELECT.views = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var views = [];
	var path = `./public/jade/`;
	
	FLEX.getIndex( path , function (files) {
		
		files.forEach( (file,n) => {
		
			var stats = FS.statSync(path + file);		
			
			if (stats.isDirectory())
				views.push( { ID: n, Name: file, class: `${file}/` } );
				
		});
	});
	
	res(views);
}

SELECT.links = function Xselect(req, res) {
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

	FLEX.getIndex( path, function (files) {

		files.forEach( file => {
		
			var stats = FS.statSync(path + file);
			
			if (stats.isDirectory()) 
				if (file.charAt(0) == ".") 
					FLEX.getIndex( path+file, function (subs) {
						subs.forEach( name => {
							links.push({
								ID: id++,
								Name: (name + " &rrarr; "+name).tag("a",{
									href: "/" + name + ".view"
								})
							});
							taken[name] = 1;
						});
					});
				
				else 
				if (false)
					FLEX.getIndex( path+file, function (subs) {
						subs.forEach( sub => {
							var name = sub.replace(".jade","");
							links.push({
								ID: id++,
								Name: ("&diams; "+name).tag("a",{
									href: "/" + file + "/" + sub
								})
							});
						});
					});
			
			else {
				var name = file.replace(".jade","");
				if ( !taken[name] )
					links.push({
						ID: id++,
						Name: name.tag("a", {
							href: "/" + name + ".view"
							/*(FLEX.statefulViews[name] ? FLEX.WORKER : "") + area + file*/
						})
					});
			}
			
		});
	});
	
	res(links);
}

/*
SELECT.themes = function (req, res) {
	var themes = [];
	var path = ENV.THEMES;

	FLEX.getIndex( path , function (files) {
		files.each( function (n,file) {
			//var stats = FS.statSync(path + "/"+file);

			if ( file.charAt(file.length-1) != "/" ) //(!stats.isDirectory())
				themes.push( {ID: n, Name: file, Path: file} );
		});
	});
	
	res(themes);	
}
*/

SELECT.summary = function Xselect(req, res) {
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

SELECT.users = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,Connects,Client AS Name, Location AS Path FROM openv.sessions WHERE least(?,1) ORDER BY Client", 
		guardQuery(query,true),
		function (err, recs) {
			res(err || recs);
	});
}

/*
SELECT.ATOMS = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,engineinfo(Name,Type,Updated,Classif,length(Code),Period,Enabled,length(Special)) AS Name FROM engines WHERE least(?,1) ORDER BY Name",
		guardQuery(query,true), 
		function (err, recs) {
			res(err || recs);
	});
}
*/

/*
SELECT.config = function Xselect(req, res) {
	var 
		sql = req.sql,
		ID = 0,
		info = [];
	
	CP.exec("df -h /dev/mapper/centos-root", function (err,dfout,dferr) {
	CP.exec("netstat -tns", function (err,nsout,nserr) {
	CP.exec("npm list", function (err,swconfig) {
		swconfig.split("\n").forEach( sw => {
			info.push({ID: ID++, Type: "sw", Config: sw, Classif: "(U)"});
		});
		
		var type="";
		nsout.split("\n").forEach( ns => {
			if (ns.endsWith(":"))
				type = ns;
			else
				info.push({ID: ID++, Type: type, Config: ns, Classif: "(U)"});
		});
			
		info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config:
			"up " + (OS.uptime()/3600/24).toFixed(2)+" days"});

		info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config:
			"%util " + OS.loadavg() + "% used at [1,2,3]" });
		
		info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config:
			"cpus " + OS.cpus()[0].model });
			
		info.push({ID: ID++, Type: "os", Classif: "(U)", Config:
			OS.type() });
		
		info.push({ID: ID++, Type: "os", Classif: "(U)", Config:
			OS.platform() });

		info.push({ID: ID++, Type: "os", Classif: "(U)", Config:
			OS.arch() });

		info.push({ID: ID++, Type: "os", Classif: "(U)", Config: 			
			OS.hostname() });	
			
		info.push({ID: ID++, Type: "disk", Classif: "(U)", Config: 
			dfout});
		
		info.push({ID: ID++, Type: "ram", Classif: "(U)", Config:
			((OS.totalmem()-OS.freemem())*1e-9).toFixed(2) + " GB " });

		info.push({ID: ID++, Type: "ram", Classif: "(U)", Config:
			(OS.freemem() / OS.totalmem()).toFixed(2) + " % used" });
		
		res(info);
		/ *
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
					+ OS.cpus()[0].model, // + " at " + ATOM.temp() + "oC",
			
			platform: OS.type()+"/"+OS.platform()+"/"+OS.arch(),
			
			memory: 
				((OS.totalmem()-OS.freemem())*1e-9).toFixed(2) + " GB " 
				+ (OS.freemem() / OS.totalmem()).toFixed(2) + " % used",
			
			host: OS.hostname() 			// do not provide in secure mode
			//cpus: JSON.stringify(OS.cpus()),
			//routes: JSON.stringify(FLEX.routes),
			//netif: JSON.stringify(OS.networkInterfaces()), 	//	do not provide in secure mode
			//temp: TEMPIF.value()  // unavail on vms
		});  * /
		
	});
	});
	});
}
*/

SELECT.datasets = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var rtns = [], ID=0;
	
	sql.eachTable( "app", function (table) {
		rtns.push({
			Name: table.tag("a",{href:"/"+table+".db"}),
			ID: ID++
		});
	});

	for (var n in SELECT)
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
		for (var n in SELECT)
			rtns.push({
				Name: n.tag("a",{href:"/"+n+".db"}),
				ID: ID++
			});
			
		res(rtns);
	}); */
}

SELECT.admin = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT *,avg_row_length*table_rows AS Used FROM information_schema.tables", [], function (err, recs) {
		res(err || recs);
	});
}

/*
SELECT.queues = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,queueinfo(Job,min(Arrived),timediff(now(),min(Arrived))/3600,Client,Class,max(State)) AS Name FROM app.queues GROUP BY Client,Class", 
		[],
		function (err, recs) {
			res(err || recs);
	});
}
*/

SELECT.cliques = function Xselect(req, res) {
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

SELECT.health = function Xselect(req, res) {
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
	.on("error", err => {
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
	.on("error", err => {
		Log(err);
	})
	.on("result", function (qstats) {
	
//Log(qstats);

	sql.query("SHOW GLOBAL STATUS", function (err,dstats) {
		
//Log(dstats);

	sql.query("SHOW VARIABLES LIKE 'version'")
	.on("result", function (vstats) {
	
		dstats.forEach( stat => { 
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
			cpu_use: (OS.loadavg()[2]*100).toFixed(2) + "% @ ", //  + ATOM.temp() + "oC",
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
			//temp: ATOM.temp()  // unavail on vms
		};
	
		nouts.forEach( (nstat,n) => {
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
			FLEX.getIndex( ENV[area], function (n,file) {
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

SELECT.likeus = function Xselect(req, res) {
	var 
		sql = req.sql, 
		log = req.log, 
		query = req.query, 
		pocs = FLEX.site.pocs;

	sendMail({
		to: pocs.admin,
		subject: req.client + " likes " + FLEX.site.title + " !!", 
		body: "Just saying"
	}, sql );

	var user = {
		expired: "your subscription has expired",
		0: "elite",
		1: "grand",
		2: "wonderful",
		3: "better than average",
		4: "below average",
		5: "first class",
		default: "limited"
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

SELECT.tips = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;

	sql.query(
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
				recs.forEach( (rec,id) => {
					rec.ID = id;
					rec.lat = rec.address[0][0].x*180/Math.PI;
					rec.lon = rec.address[0][0].y*180/Math.PI;
					//delete rec.address;
				});
			
			res(err || recs);
	});
}

SELECT.history = function Xselect(req,res) {
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
						+ [" revise your comments"].linkify(`/roles.view?client=${req.client}`)
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
						query.Datasets.split(",").forEach( dataset => {
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

							ghawk.forEach( hawk => {
								to.push( hawk.client );
							});
							lhawk.forEach( hawk => {
								cc.push( hawk.client );
							});

							if (to.length)
								sendMail({
									to: to.join(";"),
									cc: cc.join(";"),
									subject: "review completed",
									body: "for more information see " 
										+ ["moderator comments"].linkify("/moderate.view") 
										+ " and " 
										+ ["project status"].linkify("/project.view")
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

SELECT.plugins = function Xselect(req,res) {
	var sql = req.sql, query = req.query;
	var plugins = [];
	
	Log(query, isEmpty(query) );
	sql.query(
		"SELECT Name, Type, JIRA, RAS, Task "
		+ "FROM app.engines LEFT JOIN openv.milestones ON milestones.Plugin=engines.Name "
		+ ( isEmpty(query) 
		   		? "WHERE Enabled ORDER BY Type,Name"
		   		: "WHERE Enabled AND least(?,1) ORDER BY Type,Name" ),
		[query], (err,engs) => {
		
		if ( err )
			res( err );
		
		else {
			engs.forEach( (eng,id) => {
				if ( eng.Type != "jade" && eng.Name.indexOf("demo")<0 )
					plugins.push({
						Use: `/${eng.Name}.use`.tag("a",{href: `/${eng.Name}.use`}),
						Run: `/${eng.Name}.run`.tag("a",{href: `/${eng.Name}.run`}),
						View: `/${eng.Name}.view`.tag("a",{href: `/${eng.Name}.view`}),
						ToU: `/${eng.Name}.tou`.tag("a",{href: `/${eng.Name}.tou`}),
						TxStatus: `/${eng.Name}.status`.tag("a",{href: `/${eng.Name}.status`}),
						Publish: `/${eng.Name}.pub`.tag("a",{href: `/${eng.Name}.pub`}),
						Source: `/public/${eng.Type}/${eng.Name}.js`,
						Download: `/${eng.Name}.${eng.Type}`.tag("a",{href: `/${eng.Name}.${eng.Type}`}),
						Brief: `/briefs.view?options=${eng.Name}`.tag("a",{href: `/briefs.view?options=${eng.Name}`}),
						JIRA: (eng.JIRA||"").tag("a",{href:ENV.JIRA}),
						RAS: (eng.RAS||"").tag("a",{href:ENV.RAS}),
						Task: (eng.Task||"").tag("a",{href:"/milestones.view"})
					});
				/*
				sql.query("SHOW TABLES FROM ?? WHERE ?", [
					req.group, {tables_in_app: eng.Name}
				], function (err,recs) {

					if ( !err && recs.length) 
						plugins.push( {
							ID: plugins.length,
							Name: eng.Name.tag("a",{href:"/"+eng.Name+".run"}) 
						});

					if (n == engs.length-1) res( plugins );
				}); */
			});
			res( plugins );
		}
	});
	
}

/*
SELECT.DigitalGlobe = function Xselect(req, res) {  
// Digital globe interface

	var sql = req.sql, log = req.log, query = req.query;
} */

SELECT.AlgorithmService = function Xselect(req, res) { 
// Hydra interface

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

/*
SELECT.NCL = function Xselect(req, res) { 
// Reserved for NCL and ESS service alerts
	var sql = req.sql, log = req.log, query = req.query;
}

SELECT.ESS = function Xselect(req, res) { 
// Reserved for NCL and ESS service alerts
	var sql = req.sql, log = req.log, query = req.query;
}
*/

// Uploads/Stores file interface
/*
INSERT.uploads = function Xinsert(req,res) {
	const { table, type, sql, query, body } = req;
	
	Log("insert !", table, type);
	res("ok");
}*/

SELECT.uploads = 
SELECT.stores = 
function Xselect(req, res) {
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
		
			FLEX.getIndex( path, function (files) {
				
				files.forEach( (file,id) => {
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
										ID		: id
									});
									break;
									
								default:
									rtns.push({
										Name	: file.tag("a",{href:link}),
										File	: file,
										ID		: id
									});
							}
							break;

						default:
							rtns.push({
								Name	: file.tag("a",{href:link}),
								File	: file,
								ID		: id
							});
							break;
					}
				});
				
			});
			
			res(rtns);

			break;
			
		case "dir": 
		
			for (var area in {uploads:1, stores:1, shares:1} )
				FLEX.getIndex( `./public/${area}`, function (n,file) {

					var 
						link = `/${area}/${file}`,
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

/*
EXECUTE.uploads = function Xexecute(req, res) {
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
}*/

// CRUDE interfaces
// Notebook usecase editors

DELETE.keyedit = function Xdelete(req, res) { 
	var sql = req.sql, query = req.query;
	
	Log(["delkey", query]);
	try {
		sql.query(
			"ALTER TABLE app.?? DROP ??", 
			[query.ds, query.ID],
			err => {
				res( {data: {}, success:true, msg:"ok"}  );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
		
}

INSERT.keyedit = function Xinsert(req, res) { 
	var sql = req.sql, body = req.body, query = req.query;
	
	Log(["addkey",query, body]);
	try {
		sql.query(
			"ALTER TABLE app.?? ADD ?? "+body.Type,
			[query.ds, body.Key],
			err => {
				res( {data: {insertID: body.Key}, success:true, msg:"ok"} );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
}

UPDATE.keyedit = function Xupdate(req, res) { 
	var sql = req.sql, body = req.body, query = req.query, body = req.body;
	
	Log(["updkey", query, body]);
	try {
		sql.query(
			"ALTER TABLE app.?? CHANGE ?? ?? "+body.Type, 
			[query.ds, query.ID, query.ID],
			err => {
				res( {data: {}, success:true, msg:"ok"}  );
		});
	}
	
	catch (err) {
		res( FLEX.errors.badDS );
	}
	
}

SELECT.keyedit = function Xselect(req, res) { 
	var sql = req.sql, query = req.query;
	
	Log(["getkey",query]);
	
	try {
		sql.query("DESCRIBE app.??", [query.ds || ""], function (err, parms) {
			if (err) return res(err);

			var recs = [{
				ID: "XXX", Ds: query.ds, Key: "new", Type: "int(11)", 
				Samples:0, Dist:"gaus", Parms:"[0,1]" 
			}];
			
			parms.forEach( parm => {
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

EXECUTE.keyedit = function Xexecute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	
	Log(["editkey", query]);
	res("unsupported");
}

EXECUTE.milestones = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	var map = {SeqNum:1,Num:1,Hours:1,Complete:1,Task:1};

	for (var n=0;n<=10;n++) map["W"+n] = 1;

	sql.query("DELETE FROM openv.milestones");
	FLEX.RDR.xlsx(sql,"milestones.xlsx","stores",function (rec) {
		for (var n in map) map[n] = rec[n] || "";
		
		sql.query("INSERT INTO openv.milestones SET ?",map, err => {
			if (err) Log(err);
		});
	});

	res(SUBMITTED);
}

EXECUTE.sessions = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	if (FLEX.emitter)
		sql.query("SELECT * FROM openv.sessions WHERE length(Message)")
		.on("result", function (sock) {
			FLEX.emitter("alert", {msg: sock.Message, to: sock.Client, from: req.client});
			sql.query("UPDATE openv.sessions SET ? WHERE ?",[{Message:""},{ID:sock.ID}]);
		});
		
	res(SUBMITTED);
}

EXECUTE.events = function Xexecute(req, res) {
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
									levels.forEach( level => {
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

//EXECUTE.aspreqts = 
//EXECUTE.ispreqts = 
EXECUTE.issues = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
}

/*
EXECUTE.parms = function Xexecute(req, res) { 
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


EXECUTE.attrs = function Xexecute(req, res) { 
	var sql = req.sql, log = req.log, query = req.query;
	var created = [];
		
	res(SUBMITTED);

	sql.query("SELECT * FROM openv.attrs")
	.on("result", function (dsattr) {		
		sql.query("CREATE TABLE ?? (ID float unique auto_increment)", dsattr.Table);
	});
}
*/

EXECUTE.lookups = function Xexecute(req, res) {
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
	
	langs.forEach( lang => {
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

	themes.forEach( theme => {
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
EXECUTE.searches = function Xexecute(req, res) {
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
		).on("error", err => {
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
EXECUTE.chips = function Xexecute(req, res) {   
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

EXECUTE.swaps = function Xexecute(req, res) {   
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
Delivery of  "+swap._Product+" is conditional on NGA/OCIO acceptance of this deviation request.`
			}, sql);
			
		});
		
		return;
		
		// create a swap pdf
		
		sql.query("SELECT * FROM app.lookups WHERE ?",{Ref:form_name}, function (err,looks) {
			
			var form_map = {};
			
			looks.forEach( look => {
				if (look.Name)
					form_map[look.Name] = look.Path;
			});
				
			var swap_map = {};
			
			for (var n in form_map)
				swap_map[form_map[n]] = swap[n];

			/*PDF.fillForm( form_template, form_final, swap_map, err => {
				if (err) Log(`SWAP ${package} err`);
			});*/
			
			/*
			PDF.mapForm2PDF( swap, form_conv, function (err,swap_map) {
				Log("map="+err);
				
				PDF.fillForm( form_template, form_final, swap_map, err => {
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
			
			logs.split("\n").forEach( log => {
				
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
    
	/*
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
	}); */
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

function sendMail(opts, sql) {
	
	function send(opts) {
		if ( email = FLEX.mailer.TX.TRAN ) {
			opts.from = "totem@noreply.gov";
			opts.alternatives = [{
				contentType: 'text/html; charset="ISO-59-1"',
				contents: ""
			}];

			email.sendMail(opts, err => Trace(`MAIL ${opts.to} re:${opts.subject} ` + (err||"ok") ) );
		}
	}
		
	if (opts.to) 
		if ( sql ) 
			sql.query("INSERT INTO app.email SET ?", {
				To: opts.to,
				Body: opts.body,
				Subject: opts.subject,
				Send: false,
				Remove: false
			}, err => {
				
				if (err) // no email buffer provided so send it
					send(opts);
			
			});
	
		else
			send(opts);
}

//============ misc 

SELECT.follow = function (req,res) {  // follow a link
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

SELECT.login = function(req,res) {
	var 
		sql = req.sql, 
		query = req.query,
		site = FLEX.site,
		pocs = site.pocs,
		url = site.urls.worker,
		nick = site.nick,
		nickref = nick.tag("a",{href:url}),
		client = req.client,
		user = userID(client),
		profile = req.profile,
		group = profile.group,
		userHome = `/local/users/${user}`,
		logins = FLEX.paths.logins,
		sudoJoin = `echo "${ENV.ADMIN_PASS} " | sudo -S `,
		isp = {
			machine: ENV.SERVICE_WORKER_URL,
			admin: pocs.admin,
			overlord: pocs.overlord,
			remotein: `${logins}/${user}.rdp`,
			sudos: [
				`adduser ${user} -M --gid ${group} -p ${ENV.LOGIN_PASS}`,
				`usermod -d ${userHome} ${user}`,
				`id ${user}`,
				`mkdir -p ${userHome}/.ssh`,
				`cp ./certs/${user} ${userHome}/.ssh`,
				`cp ${logins}/template.rdp ${logins}/${user}.rdp`,
				`ln -s /local/service ${userHome}/totem`
			],				
			hosted: false
		},
		notice = `
Greetings from ${nickref}-

Your TOTEM development login ${user} has been created for ${client}.

${isp.admin}: Please create an AWS EC2 account ${user} for ${client} using the attached cert.

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
	
	if (client == "guest")
		res( FLEX.errors.badLogin );
	
	else
	if ( !ENV.PASS )
		res( FLEX.errors.failedLogin );
	
	else
	if ( profile.User )  // already have valid user id so just remote in
		res( function () { return isp.remotein; } );
	
	else
	if ( createCert = FLEX.createCert )
		createCert(user, pass, function () {  // register new userID, certs, login account, and home path
			var 
				prep = isp.sudos.join(";"),
				prepenv = sudoJoin + isp.sudos.join("; "+sudoJoin);

			Trace(`CREATE LOGIN FOR ${client} PREP ${prep}`, sql);

			CP.exec( prepenv, function (err,out) {
				if (err)  {
					res( FLEX.errors.failedLogin );
					sendMail({
						to: isp.admin,
						cc: isp.overlord,
						subject: `${nick} login failed`,
						body: err + `This is an automatted request from ${nick}.  Please provide ${isp.overlord} "sudo" for ${prep} on ${isp.machine}`
					}, sql);
				}

				else  {
					res( function () { return isp.remotein; } );
					sql.query("UPDATE openv.profiles SET ? WHERE ?", [{User: user}, {Client: client}]);
					sendMail({
						to: client,
						cc: `${isp.overlord};${isp.admin}`,
						subject: `${nick} login established`,
						body: notice
					}, sql);
				};
			});
		});
	
	else	
		res( FLEX.errors.failedLogin );
	
}

function userID(client) {
	var 
		parts = client.split("@"),
		parts = (parts[0]+".x.x").split("."),
		user = parts[0].charAt(0)+parts[1].charAt(0)+parts[2];
	
	return user.substr(0,8).toLowerCase();
}

SELECT.proctor = function (req,res) {  //< grade quiz results
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
	}], err => {
		
		sql.forAll(
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
						{Client:client, Topic:topic} ).end();
					
					sendMail({
						to: client,
						subject: `${site.nick} sends its congradulations`,
						body: 
							`you completed all ${topic} modules and may claim your `
							+ "certificate".tag("a", {href:`/stores/cert_${topic}_${client}.pdf`})
					}, sql );
				}
				
		});
	
	});
}
		
SELECT.sss = function (req,res) {
	res("reserved");
}

SELECT.wms = function (req,res) {
	
	var 
		sql = req.sql,
		query = req.query,
		probeSite = FLEX.probeSite,
		src = query.src || "";
	
	switch (src) {
		case "dglobe":
		case "omar":
		case "ess":
		default:
	}
	
	res("ok");
	
	if ( url = ENV[`WMS_${src.toUpperCase()}`] ) 
		probeSite(url.tag("?", query), rtn => Log("wms returned", rtn) )
}

SELECT.wfs = function (req,res) {  //< Respond with ess-compatible image catalog
	var 
		sql = req.sql,
		query = req.query,
		probeSite = FLEX.probeSite,
		site = FLEX.site,
		chip = {	// chip attributes
			width: query.width || 100, // chip pixels lat,
			height: query.height || 100, // chip pixels lon,
			srs: "epsg%3A4326",
			format: "image/jpeg"
		},
		ring = query.ring || [],		// aoi ring
		chipper = "wms",  // wms || jpip || wmts
		src = (query.src || "").toUpperCase();	// catalog service
	
	switch (src) {
		case "DGLOBE":
		case "OMAR":
		case "ESS":
		default:
			//query.geometryPolygon = JSON.stringify({rings: JSON.parse(query.ring)});  // ring being monitored
			query.geometryPolygon = JSON.stringify({rings: ring});  // ring being monitored
	}

	delete query.src;
	
	if ( url = ENV[`WFS_${src}`] )
		probeSite( url.tag("?", query), cat => {  // query catalog for desired data channel
			if ( cat = cat.parseJSON() ) {
				switch ( src ) {  // normalize cat response to ess
					case "DGLOBE":
						// tbd
					case "OMAR":
						// tbd
					case "ESS":
						var
							recs = ( cat.GetRecordsResponse || {SearchResults: {}} ).SearchResults,
							collects = recs.DatasetSummary || [],
							sets = [];

						collects.forEach( collect => {  // pull image collects from each catalog entry

							//Log(collect);
							var 
								image = collect["Image-Product"].Image,
								imageID = image.ImageId.replace(/ /g,""),
								sun = image["Image-Sun-Characteristic"] || {SunElevationDim: "0", SunAzimuth: "0"},
								restrict = collect["Image-Restriction"] || {Classification: "?", ClassificationSystemId: "?", LimitedDistributionCode: ["?"]},
								raster = image["Image-Raster-Object-Representation"],
								region = collect["Image-Country-Coverage"] || {CountryCode: ["??"]},
								atm = image["Image-Atmospheric-Characteristic"],
								urls = {
									wms: collect.WMSUrl,
									wmts: collect.WMTSUrl,
									jpip: collect.JPIPUrl
								};

							if (urls.wms)  // valid collects have a wms url
								sets.push({	
									imported: new Date(image.ImportDate),
									collected: new Date(image.QualityRating),
									mission: image.MissionId,
									sunEl: parseFloat(sun.SunElevationDim),
									sunAz: parseFloat(sun.SunAzimuth),
									layer: collect.CoverId,
									clouds: atm.CloudCoverPercentageRate,
									country: region.CountryCode[0],
									classif: restrict.ClassificationCode + "//" + restrict.LimitedDistributionCode[0],
									imageID: imageID,
											// "12NOV16220905063EA00000 270000EA530040"
									mode: image.SensorCode,
									bands: parseInt(image.BandCountQuantity),
									gsd: parseFloat(image.MeanGroundSpacingDistanceDim)*25.4e-3,
									wms: urls[chipper]
											.replace("http:", "wget:")
											.replace("https:", "wgets:")
											.replace(
												"?REQUEST=GetCapabilities&VERSION=1.3.0",
												"?request=GetMap&version=1.1.1")

											.tag("&", chip)
											+ "////"	// wget output path
											+ FLEX.paths.chips + imageID
								});

						});
					}

				res( sets );
			}

			else
				res( "" );
		});	
	
	else
	if ( src == "SPOOF" ) // spoof
		res([{			
			imported: new Date(),
			collected: new Date(),
			mission: "debug", //image.MissionId,
			sunEl: 0, //parseFloat(sun.SunElevationDim),
			sunAz: 0, //parseFloat(sun.SunAzimuth),
			layer: "debug", //collect.CoverId,
			clouds: 0, //atm.CloudCoverPercentageRate,
			country: "XX", //region.CountryCode[0],
			classif: "", //restrict.ClassificationCode + "//" + restrict.LimitedDistributionCode[0],
			imageID: "ddxxxyymmmmxxxxxEAnnnnn xxxxxEAnnnnnn",
					// "12NOV16220905063EA00000 270000EA530040"
			mode: "XX", //image.SensorCode,
			bands: 0, //parseInt(image.BandCountQuantity),
			gsd: 0, //parseFloat(image.MeanGroundSpacingDistanceDim)*25.4e-3,
			wms: "" + "////" + FLEX.paths.chips + "spoof.jpg"	
			/*{
			GetRecordsResponse: {
				SearchResults: {
					DatasetSummary: [{
						"Image-Product": {
							Image: {
								ImageId: "spoof",
								"Image-Sun-Characteristic": {
									SunElevationDim: "0", 
									SunAzimuth: "0"
								},
								"Image-Raster-Object-Representation": [],
								"Image-Atmospheric-Characteristic": []
							},
							"Image-Restriction": {
								Classification: "?", 
								ClassificationSystemId: "?", 
								LimitedDistributionCode: ["?"]
							},
							"Image-Country-Coverage": {
								CountryCode: ["??"]
							}
						},
						WMSUrl: ENV.SRV_TOTEM+"/shares/spoof.jpg",
						WMTSUrl: "",
						JPIPUrl: ""
					}]
				}
			}
		} */
		}]);
	
	else
		res( "" );

}

SELECT.matlab = function (req,res) {
	var
		sql = req.sql,
		query = req.query;
	
	res("matlab queue flushed");		
}

SELECT.help = function (req,res) {
	var 
		sql = req.sql,
		query = req.query,
		from = query.from,
		site = FLEX.site;
	
	res("your name has been added to the automatted, class-action remedy tickit");
	switch (from) {
		case "pmo":
		case "asp":
		case "isp":
		case "swap":
	}
	
}

SELECT.filestats = function (req,res) {
	var 
		sql = req.sql,
		query = req.query;
	
	var q = sql.query(
		"SELECT voxels.ID, _stats.*,geomfromtext(concat('POINT(', voxels.lon, ' ', voxels.lat,')')) AS Location, voxels.radius AS Radius, "
		+ "app.link(files.Name,concat('/files.view',char(63),'name=',files.Name)) AS Source "
		+ "FROM app._stats "
		+ "LEFT JOIN app.files ON files.ID = _stats.fileID "
		+ "LEFT JOIN app.voxels ON voxels.ID = _stats.voxelID "
		+ "HAVING voxels.ID",
		[], function (err,recs) {
			
		//Log(err);
		res( err || recs );
	});
	//Log(q.sql);	
}

/*
function sysAgent(req,res) {
	var 
		query = req.query,
		cb = ATOM.mw.cb[query.job];
	
	Log("AGENT", query);
	cb(0);
}

function sysConfig(req,res) {
/ **
@method sysConfig
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
* /
	function Guard(query, def) {
		for (var n in query) return query;
		return def;
	}
	
	var query = Guard(req.query,false);
	
	if (query)
		req.sql.query("UPDATE config SET ?", query, err => {
			res( err || "parameter set" );
		});
}

function sysCheckpt(req,res) {
/ **
@method sysCheckpt
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
* /
	CP.exec('source maint.sh checkpoint "checkpoint"');
	res("Checkpointing database");
}

function sysStart(req, res) {
/ **
@method sysStart
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
* /
	req.sql.query("select * from openv.apps where least(?)",{Enabled:true,Name:req.query.name||"node0"})
	.on("result",function (app) {
		if (false)
			CP.exec("node $EXAPP/sigma --start "+app.Name, function (err,stdout,stderr) {
				if (err) console.warn(err);
			});
		else
			process.exit();				
	})
	.on("end", function () {
		res("restarting service");
	});
}

function sysBIT(req, res) {
/ **
@method sysBIT
Totem(req,res) endpoint for builtin testing
@param {Object} req Totem request
@param {Function} res Totem response
* /
	var 
		N = req.query.N || 20,
		lambda = req.query.lambda || 2;
	
	var
		actions = ["insert","update","delete"],
		tables = ["test1","test2","test3","test4","test5"],
		users = ["simuser1","simuser2","simuser3","simuser4","simuser5"],
		f1 = ["sim1","sim2","sim3","sim4","sim5","sim6","sim7","sim","sim9","sim10","sim11","sim12","sim13"],
		f2 = ["a","b","c","d","e","f","g","h"],
		f3 = [0,1,2,3,4,5,6,7,,9,10];

	var t0 = 0;

	// Notify startup
	
	//Trace(`BIT ${N} events at ${lambda} events/s with logstamp ${stamp}`);
	
	res("BIT running");

	// Setup sync for server blocking and notify both sides
	
	FLEX.BIT = new SYNC(N,{},function () { 
		FLEX.BIT = null; 
		Trace("BIT completed");
	});
	
	//DEBE.LOGSTAMP = Stamp;
	
	// Poisson load model.
	
	for (var n=0;n<N;n++) {
		var t = - 1e3 * Math.log(Math.random()) / lambda;			// [ms] when lambda [1/s]
		
		t0 += t;

		var taskID = setTimeout(function (args) {
			req.body = clone(args.parms);
			req.query = (args.action == "insert") ? {} : {f1: args.parms.f1};
			req.ses.source = "testdb."+args.table;
			req.ses.action = args.action;

			FLEX.open(req,res);  		// need cb?			
		}, t0, {	
			parms: {f1:f1.rand(),f2:f2.rand(),f3:f3.rand()}, 
			table: tables.rand(), 
			action: actions.rand(),
			client: users.rand()
		});
	}
}

function sysCodes(req,res) {
/ **
@method sysCodes
@deprecated
Totem(req,res) endpoint to return html code for testing connection
@param {Object} req Totem request
@param {Function} res Totem response
* /
	res( HTTP.STATUS_CODES );
}

function sysKill(req,res) {
/ **
@method sysKill
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
* /
	var killed = [];

	res("Killing jobs");

	req.sql.query("SELECT * FROM app.queues WHERE pid AND LEAST(?,1)", req.query)
	.on("result", function (job) {
		req.sql.query("UPDATE queues SET ? WHERE ?", [{
			Notes: "Stopped",
			pid: 0,
			Departed: new Date()}, 
			{ID: job.ID} ]);

		CP.exec("kill "+job.pid);
	});
}
*/

/*
SELECT.help = function sysHelp(req,res) {
/ **
@method sysHelp
Totem(req,res) endpoint to return all sys endpoints
@param {Object} req Totem request
@param {Function} res Totem response
* /
	res(
		  "/ping.sys check client-server connectivity<br>"
		+ "/bit.sys built-in test with &N client connections at rate &lambda=events/s<br>"
		+ "/codes.sys returns http error codes<br>"
		+ "/alert.sys broadcast alert &msg to all clients<br>"
		+ "/stop.sys stops server with client alert &msg<br>"
	);
}
*/

EXECUTE.gitreadme = function(req,res) {
	res("git commiting and push");
}

SELECT.status = function (req,res) {
	var
		sql = req.sql,
		query = req.query,
		now = new Date(),
		year = query.year || now.getFullYear(),
		week = now.getWeek(),
		from = parseInt(query.from || "0") || week,
		weeks = parseInt(query.weeks || "0"),
		to = parseInt(query.to || "0") || (from+weeks),
		_year = "_" + year,
		compress = query.compress,
		interp = query.interp,
		acts = {
			"\n": "",
			_I: "installed",
			_C: "created",
			_F: "fixed",
			_D: "documented",
			_B: "built",
			_T: "transitioned",
			_R: "removed",
			_T: "terminated"
		},
		rtns = [],
		rtn = {ID: 1};
	
	Log(year,week,from,to);
	
	if ( reader = READ.readers.xls_Reader )
		reader(sql, FLEX.paths.status, recs => {

			if (recs ) {			
				Each( recs[0], (key,val) => {  // remove cols outside this year
					if ( key.startsWith("_") )
						if ( !key.startsWith(_year) )
							recs.forEach( rec => delete rec[key] );
				});

				if (from) 	// remove cols before from-week
					for (var n=1,N=from; n<N; n++)  
						recs.forEach( rec => delete rec[ _year+"-"+n] );

				if (to)		// remove cols after to-week
					for (var n=to+1, N=99; n<N; n++)
						recs.forEach( rec => delete rec[ _year+"-"+n] );

				var fill = new Object(recs[0]);

				recs.forEach( (rec,idx) => {  // scan all records
					if (idx) {	// not the header record
						var task = "", use = true, testable = false;

						if (interp)   // interpolate missing keys
							Each(fill, (key,val) => {
								if ( key.startsWith(".") )
									if ( val = rec[key] )
										fill[key] = val;

									else
										rec[key] = fill[key];
							});

						Each(rec, (key,val) => {  // see if this record can be used
							if ( key.startsWith(".") ) {
								testable = true;
								if ( test = query[ key.substr(1).toLowerCase() ] ) 
									if ( val ) 
										use = test.toLowerCase() == val.toLowerCase();
									else
										use = false;
							}
						});

						if (use && testable) 
							if (compress)   // produce compressed output
								Each(rec, (key,val) => {
									if (val)
										switch (key) {
											case "ID":
											case "sheet":
												break;

											default:
												Each( acts, (act, sub) => {  // expand shortcuts
													val = val.replace( new RegExp(act,"g"), sub);
												});

												if ( key.startsWith("_") && task )
													switch (compress) {
														case "robot":
															rtn[key] += `> ${task}: ${val}`;
															break;

														case "human":
															var 
																parts = task.split("."),
																object = parts.pop(),
																type = parts.pop(),
																service = parts.pop(),
																effort = parts.pop();

															//Log(task, parts);

															rtn[key] += `> ${val} the ${object} ${type} for the ${effort} ${service} effort`;
															break;

														default:
															rtn[key] = "invalid compress option";														
													}

												else
													task += "."+val;
										}
								});

							else	// append record to returned
								rtns.push( new Object(rec) );
					}

					else  // use header record to prime returns
						Each(rec, (key,val) => {
							if ( key.startsWith("_") ) rtn[key] = "";
						});
				});

				res( compress ? [rtn] : rtns );
			}

			else
				res( new Error( `could not find ${FLEX.paths.status}` ) );

		});
	
	else
		res( new Error( "missing xls reader" ));
}

/*
SELECT.test = function(req,res) {
	req.sql.serialize([{save: "news"},{save:"lookups"}, {save:"/news"} ], {}, res );
} */

/*
INSERT.blog = function (req,res) {
	var
		query = req.query;
	
	switch (req.type) {
		case "mu":
			res( req.post.parseEMAC( productKeys( "nill", query ) ) );
			break;
			
		case "":
			req.post.Xblog(req, query.ds || "nada?id=0", {}, productKeys( "nill", query) , {}, false, html => res(html) );
			break;
			
		case "py":
		case "js":
			res( "not implemented" );
			break;
			
	}			
}  */

SELECT.os = function (req,res) {
	var
		sql = req.sql,
		query = req.query;
	
	CP.exec(
		"df -h /dev/mapper/centos-root", 
		(err,dfout,dferr) => {
			
		var info = [];
			
		if (!err) {
			info.push({ID: ID++, Type: "disk", Classif: "(U)", Config:  dfout});
			info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config: "up " + (OS.uptime()/3600/24).toFixed(2)+" days"});
			info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config: "%util " + OS.loadavg() + "% used at [1,2,3]" });
			info.push({ID: ID++, Type: "cpu", Classif: "(U)", Config: "cpus " + OS.cpus()[0].model });
			info.push({ID: ID++, Type: "os", Classif: "(U)", Config: OS.type() });
			info.push({ID: ID++, Type: "os", Classif: "(U)", Config: OS.platform() });
			info.push({ID: ID++, Type: "os", Classif: "(U)", Config: OS.arch() });
			info.push({ID: ID++, Type: "os", Classif: "(U)", Config: OS.hostname() });	
			info.push({ID: ID++, Type: "ram", Classif: "(U)", Config: ((OS.totalmem()-OS.freemem())*1e-9).toFixed(2) + " GB " });
			info.push({ID: ID++, Type: "ram", Classif: "(U)", Config: (OS.freemem() / OS.totalmem()).toFixed(2) + " % used" });
		}
			
		res(info);
	});
};

SELECT.net = function (req,res) {
	CP.exec("netstat -tns", function (err,nsout,nserr) {
		var type="", info = [];
		
		if (!err)
			nsout.split("\n").forEach( ns => {
				if (ns.endsWith(":"))
					type = ns;
				else
					info.push({ID: ID++, Type: type, Config: ns, Classif: "(U)"});
			});			
		
		res(info);
	});
};

SELECT.config = function (req,res) {
	var
		sql = req.sql,
		query = req.query,
		probeSite = FLEX.probeSite;
	
	if (mod = query.mod) 
		CP.exec(`cd ../${mod}; npm list`, function (err,swconfig) {			
			var info = [];
			
			if (!err)
				swconfig.split("\n").forEach( sw => {
					sw = escape(sw)
						.replace(/\%u2502/g,"")
						.replace(/\%u2500/g,">")
						.replace(/\%u251C/g,"")
						.replace(/\%u252C/g,"")
						.replace(/\%u2514/g,"")
						.replace(/\%0A/g,"")
						.replace(/\%20/g,"");

					info.push({ID: info.length, Type: "sw", Config: sw, Classif: "(U)"});
				});

			res(info);
		});		
	
	else 
		probeSite( "/config?mod=enum", en => {
		probeSite( "/config?mod=flex", flex => {
		probeSite( "/config?mod=totem", totem => {
		probeSite( "/config?mod=debe", debe => {
			res({
				enum: en.parseJSON([]),
				flex: flex.parseJSON([]),
				totem: totem.parseJSON([]),
				debe: debe.parseJSON([])
			});
		}); }); }); });
};

SELECT.info = function (req,res) {
	function toSchema( path, obj ) {
		if ( isObject(obj) ) {
			var objs = [];
			Each(obj, (key,val) => {
				var next = path+"/"+key;
				
				if (val)
					switch (val.constructor.name) {
						case "String":
							if ( val.startsWith("<") )
								objs.push({
									name: key, 
									size: 10,
									doc: next + ": " + val ,
									children: toSchema(next, val)
								});
							
							else
							if ( val.startsWith("http") || val.startsWith("mailto") )
								objs.push({
									name: key, 
									size: 10,
									doc: next + ": " + key.tag( val ),
									children: toSchema(next, val)
								}); 
							
							else
							if ( val.startsWith("/") )
								objs.push({
									name: key, 
									size: 10,
									doc: next + ": " + key.tag( val ),
									children: toSchema(next, val)
								}); 

							else
								objs.push({
									name: key, 
									size: 10,
									doc: next + ": " + val,
									children: toSchema(next, val)
								}); 
								
							break;
							
						case "Array":
							objs.push({
								name: key + `[${val.length}]`, 
								size: 10,
								doc: next,
								children: toSchema(next, val)
							}); break;
						
						case "Object":
							objs.push({
								name: key + ".", 
								size: 10,
								doc: next,
								children: toSchema(next, val)
							}); break;
						
						case "Function":
							objs.push({
								name: key + "(...)", 
								size: 10,
								doc: next.tag( "/api.view" ),
								children: []
							}); break;
							
						default:
							objs.push({
								name: key, 
								size: 10,
								doc: "",
								children: []
							}); break;
					}
			});
			return objs;
		}
		
		else 
			return [];
	}

	var
		sql = req.sql,
		query = req.query,
		pathPrefix = FLEX.site.urls.master,
		probeSite = FLEX.probeSite,
		libs = {
			misc: {},
			generators: {},
			regressors: {
				train: {},
				predict: {}
			}
		},
		tables = {}, 
		xtables = {},
		ID = 0,
		info = [];		
	
	Each( $.extensions, (name,ex) => {		
		if ( isFunction(ex) ) {
			var 
				parts = name.split("_"),
				reg = libs.regressors[ parts.pop() ];

			if ( reg ) 
				reg[ name ] = name;

			else
			if ( name.endsWith( "dev" ) )
				libs.generators[ name ] = name;

			else					
				libs.misc[ name ] = name;
		}
	});

	sql.query("SHOW TABLES FROM app")
	.on("result", rec => {
		if ( name = rec.Tables_in_app )
			if ( !name.startsWith("_") )
				if ( name.indexOf("demo") < 0 )
					tables[ name ] = name.tag( `/${name}` );
	})
	.on("end", (x) => {
		Each( SELECT, table => {
			xtables[ table ] = table.tag( `/${table}` );
		});

		probeSite( "/plugins", plugins => {
		probeSite( "/config", config => {
			var plugs = {};

			plugins.parseJSON( [] ).forEach( plug => plugs[plug.Name] = plug );

			res( toSchema( "", {
				totem: {
					providers: {
						research: {
							StanfordUniv: "https:www.stanford.edu",
							CarnegieMellonUniv: "https:www.cmu.edu",
							PennStateUnic: "https:www.penstateind.com",
							OxfordUniv: "https:www.ox.ac.uk/",
							FloridaStateUnic: "https:www.fsu.edu",
							UnivMaryland: "https://www.umuc.edu",
							KoreaAgencyForDefenseDevelopment: "https://en.wikipedia.org/wiki/Agency_for_Defense_Development"
						},
						"s/w": {
							opencv: {
								torch: "https://pytorch.org/",
								tensorflow: "https://www.tensorflow.org/",
								caffe: "https://caffe.berkeleyvision.org/"
							},
							anaconda: "http://anaconda.com/distribution",							
							github: "https://github.com/996icu/996.ICU",
							npm: "https://www.npmjs.com/"				
						},
						data: {
							high: {
								blitz: "https://blitz.ilabs.ic.gov",
								usaf: "https://b.nro.ic.gov",
								bvi: "https://bvi.ilabs.ic.gov",
								proton: "tbd",
								icon: "tbd",
								chrome: "tbd",
								irevents: "https://tada.ic.gov",
								ess: "https://ess.nga.ic.gov",
								thresher: "https://thresher.ilabs.ic.gov"
							},
							low: {
							}
						},
						DTOs: {
							mexican: {
								guadalajara: "x",
								sinola: {
									colima: "x",
									sonora: "x",
									artistas: "x",
									"gente nueva": "x",
									"los antrax": "x"
								},
								"beltran-leyva": {
									"los negros": "x",
									"south pacific": "x",
									"del centro": "x",
									"independence de acapulco": "x",
									"la barredora": "x",
									"el comando del diablo": "x",
									"la mano con ojos": "x",
									"la nueva administracion": "x",
									"la oficina": "x"
								},
								gulf: {
									"los zetas": "x",
									"la familia michoacana": "x",
									"knights templar" : "x",
									"los caballeros": "x"
								},
								juarez: "x",
								tijuana: "x",
								"barrio azteca": "x",
								"milenio": {
									"la resistancia": "x",
									"jalisco new generation": "x"
								}
							}
						},
						ISP: {
							dev: {
								"RLE High": "mailto:poc",
								"RLE Low": "mailto:poc"
							},
							"target op": {
								blitz: "https://blitz.ic.gov",
								proton: "https://protoon.ic.gov"
							}
						}
					},
					project: {
						plugins: plugs,
						RTP: "/rtpsqd.view?task=seppfm",
						JIRA: "JIRA",
						RAS: "RAS"
					},
					service: {
						api: "/api.view",
						"skinning guide": "/skinguide.view",
						requirements: "/project.view",
						codebase: config.parseJSON(),
						developers: {
							repos: ENV.PLUGIN_REPO,
							login: "/shares/winlogin.rdp",
							prms: {
								debe: "/shares/prm/debe/index.html",
								totem: "/shares/prm/totem/index.html",
								atomic: "/shares/prm/atomic/index.html",
								man: "/shares/prm/man/index.html",
								flex: "/shares/prm/flex/index.html",
								geohack: "/shares/geohack/man/index.html"
							}
						},
						tables: {
							flex: xtables,
							sql: tables
						}
					},
					plugins: {
						libs: libs,
						published: plugs
					}
				}
			}) );
		}); });
	});	
};

/*
SELECT.gen = function (req, res) {
	Log("gen", req.query);
	$.gen(req.query, evs => {
		//Log("gen", evs);
		res({
			x: evs.x._data,
			y: evs.y._data
		});
	});
};  */

SELECT.costs = function (req,res) {
	const {sql,query} = req;
	const {years, lag, vms} = query;
	var 
		docRate = 10e3,	// docs/yr
		doc$ = 100e3*(2/2e3), // $/doc
		minYr = 60*24*365, // mins/yr
		cycleT = 5, // mins
		boostT = 1,  // mins per boost cycle
		batch = 5e3, 	// docs/cycle
		Rcycles = docRate * lag / batch,	// cycles/yr in R state
		Ocycles = docRate * 1 / batch,	// cycles/yr in Oper state
		vmU = n => (cycleT + boostT*n) / minYr, 	// cpu utilization
		vm$ = n => 5e3 * vms * vmU(n),	// $/cycle
		R$ = n => vm$(n) * Rcycles, 		// $/yr
		O$ = n => vm$(n) * Ocycles,			// $/yr
		labU = 0.1,		// % docs labelled
		lab$ = docRate * labU * (0.25/2e3),	// $/yr
		x = [ 0 ],
		y = [ [R$(1), doc$, 0] ];
	
	for (var n = 1; n<years; n++) {
		var 
			$ = lab$ + O$(n),  	// $/yr
			$doc = $/docRate,	// $/doc process
			y0 = y[n-1][0]+$,		// cum proc
			y1 = y[n-1][1]+doc$; 	// cum acq
				
		x.push( n );
		y.push( [y0, y1, y1/y0] );
	} 
	
	res([x,y]);
};

[
	function mailify( label, tags ) {
		return this ? label.tag( "mailto:"+this.tag("?", tags || {}) ) : "missing email list";
	}
].Extend(String);

//======================= unit tests

switch ( process.argv[2] ) { //< unit tests
	case "?":
		Log("F1");
		break;
}

// UNCLASSIFIED
