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
@requires stream

@requires enum
@requires atomic
@requires reader

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
*/
 
var 	
	// globals
	TRACE = "X>",
	ENV = process.env, 					// external variables
	SUBMITTED = "submitted",

	// nodejs bindings
	VM = require('vm'), 				// V8 JS compiler
	STREAM = require("stream"), 	// pipe-able streams
	CLUSTER = require('cluster'),		// Support for multiple cores	
	HTTP = require('http'),				// HTTP interface
	CRYPTO = require('crypto'),			// Crypto interface
	//NET = require('net'), 				// Network interface
	URL = require('url'), 				// Network interface
	FS = require('fs'),					// Need filesystem for crawling directory
	CP = require('child_process'), 		// Child process threads
	OS = require('os'),					// OS utilitites

	// 3rd party bindings
	JSMIN = require("uglify-js"), 			// code minifier
	HTMLMIN = require("html-minifier"), // html minifier
	//PDF = require('pdffiller'), 		// pdf form processing
	MAIL = require('nodemailer'),		// MAIL mail sender
	SMTP = require('nodemailer-smtp-transport'),
	IMAP = require('imap'),				// IMAP mail receiver
	FEED = require('feed'),				// RSS / ATOM news feeder
	//RSS = require('feed-read'), 		// RSS / ATOM news reader

	// totem bindings
	ATOM = require("atomic"), 		// tauif simulation engines
	//RAN = require("randpr"), 		// random process
	READ = require("reader");

const { Copy,Each,Log } = require("enum");

var
	FLEX = module.exports = {
		
		mailer : {						// Email parameters
			TRACE 	: true,	
			SOURCE: "tbd",
			TX: {},
			RX: {}
		},
		
		defDocs: {
			Export: "switch writes engine results into a file [api](/api.view)",
			Ingest: "switch ingests engine results into the database",
			Share: "switch returns engine results to the status area",
			Pipe: `
json regulates chips and events to the engine:

file: "/NODE" || "PLUGIN.CASE" || "FILE.TYPE" || "FILE?QUERY" || [ {x,y,z,t,u,n, ...}, ... ]
group: "KEY,..." || ""  
where: { KEY: VALUE, ...} || {}  
order: "KEY,..." || "t"  
limit: VALUE || 1000  
task: "NAME" || ""  
aoi: "NAME" || [ [lat,lon], ... ] || []

`,
			Description: `
blog markdown documents a usecase:

[ post ] ( SKIN.view ? w=WIDTH & h=HEIGHT & x=KEY$EXPR & y=KEY$EXPR & src=DS & id=VALUE )  
[ image ] ( PATH.jpg ? w=WIDTH & h=HEIGHT )  
[ TEXT ]( LINK )  
[ FONT ]( TEXT )  
!$ inline TeX $  ||  $$ break TeX $$ || a$ AsciiMath $ || m$ MathML $  
#{ KEY } || #{ KEY }( SHORTCUT ) || ={ KEY } || !{ EXPR }  || \${ KEY as TeX matrix  }  
#TAG

`,

			Config: "js-script defines a usecase context  ",
			Save: "json aggregates engine results not captured in other Save_KEYs  ",
			Entry: 'json primes context KEYs on entry using { KEY: "SELECT ....", ...}  ',
			Exit: 'json saves context KEYs on exit using { KEY: "UPDATE ....", ...}  ',
			Batch: "value overrides the supervisor's batch size  (0 disabled)  ",
			Symbols: "json overrides the supervisor's state symbols (null defaults)  ",
			Steps: "value overrides the supervisor's observation interval (0 defaults) "
		},

		licenseOnDownload: true,
		licenseOnRestart: false,
		
		serviceID: function (url) {
			return CRYPTO.createHmac("sha256", "").update(url || "").digest("hex");
		},
		
		licenseCode: function (sql, code, pub, cb ) {  //< callback cb(pub) or cb(null)

			function returnLicense(pub) {
				var
					product = pub.Product,
					endService = pub.EndService,
					parts = product.split("."),
					type = parts.pop(),
					name = parts.pop(),
					secret = product + "@" + endService;
				
				FLEX.genLicense( code, product, type, secret, (minCode, license) => {
					
					if (license) {
						cb( Copy({
							License: license,
							EndServiceID: FLEX.serviceID( pub.EndService ),
							Copies: 1
						}, pub) );

						sql.query( "INSERT INTO app.releases SET ? ON DUPLICATE KEY UPDATE Copies=Copies+1", pub );
						
						sql.query( "INSERT INTO app.masters SET ? ON DUPLICATE KEY UPDATE ?", [{
							Master: minCode,
							License: license,
							EndServiceID: pub.EndServiceID
						}, {Master: minCode} ] );
					}

					else 
						cb( null );

				});
			}
			
			if (endService = pub.EndService)
				FLEX.fetcher( endService, null, null, (rtn) => {  // validate end service
					if (rtn) 
						returnLicense(pub);
					
					else
						cb( null  );
				});	
			
			else
				returnLicense(pub);
		},
		
		publishPlugin: function (sql, name, type, relicense) {  // publish product = name.type
			
			function getter( opt, def ) {	
				if (opt) 
					if ( opt.name == "get" )
						return opt( FS.readFileSync , CP.exec );
					else
						return opt+"";
				else
					return def || "";
			}
			
			var 
				resetAll = false,
				paths = FLEX.paths.publish,
				product = name + "." + type,
				site = FLEX.site,
				pathname = paths[type] + name;
			
			// Log("PUBLISH", pathname, process.cwd());

			try {
				var	mod = require(process.cwd() + pathname.substr(1));
			}
			catch (err) {
				Log("PUBLISH", name, err);
				return;
			}

			// FLEX.execute[name] = runPlugin;

			var
				defs = { 
					tou: FS.readFileSync( "./public/tou.md", "utf8" ),
					subs: Copy( mod.subs || {}, {
						NAME: name.toUpperCase(),
						by: "[org](atorg)",
						poc: "[poc](mailto:poc?subject=tbd&body=tbd)",
						compreqts: "tbd",
						summary: "tbd",
						name: name,
						type: type,
						product: product,
						ver: "tbd",
						now: new Date()
					})
				};
			
			if ( mod.clear || mod.reset )
				sql.query("DROP TABLE app.??", name);

			sql.query( 
				`CREATE TABLE app.${name} (ID float unique auto_increment, Name varchar(32) unique key)` , 
				[], function (err) {

				var docs = Copy( FLEX.defDocs, mod.docs || {}) ;

				if ( keys = mod.modify || mod.mods || (resetAll ? mod.keys || mod.usecase : null) || mod._mods)
					Each( keys, function (key,type) {
						if ( doc = docs[key] )
							doc.renderBlog({now:new Date()}, "", function (html) {
								sql.query( `ALTER TABLE app.${name} MODIFY ${key} ${type} comment ?`, [html] );
							});

						else								
						if (mod._mods)
							if (key.charAt(0) == "_")
								sql.query( `ALTER TABLE app.${name} CHANGE ${key.substr(1)} ${key} ${type}`, (err) => Log(err) );
							else
								sql.query( `ALTER TABLE app.${name} MODIFY ${key} ${type}` );
						else							
							sql.query( `ALTER TABLE app.${name} MODIFY ${key} ${type}` );
					});

				else
				if ( keys = mod.usecase || mod.keys || mod.adds )
					Each( keys, function (key,type) {
						if ( doc = docs[key] )
							doc.renderBlog({now:new Date()}, "", function (html) {
								sql.query( `ALTER TABLE app.${name} ADD ${key} ${type} comment ?`, [html] );
							});

						else
							sql.query( `ALTER TABLE app.${name} ADD ${key} ${type}` );
					});

				if ( inits = mod.inits || mod.initial || mod.initialize ) 
					inits.forEach( function (init, idx) {
						sql.query("INSERT INTO app.?? SET ?", init);
					});

				if  ( readme = mod.readme )
					FS.writeFile( pathname+".xmd", readme, "utf8" );
			});

			if ( code = getter( mod.engine || mod.code) ) {

				if ( relicense ) 
					FLEX.licenseCode( sql, code, {
						EndUser: "totem",
						EndService: ENV.SERVICE_MASTER_URL,
						Published: new Date(),
						Product: product,
						Path: pathname
					}, (pub) => {

						if (pub)
							Log("LICENSED", pub);
					});

				var 
					from = type,
					to = mod.to || from,
					fromFile = pathname + "." + from,
					toFile = pathname + "." + to,
					rev = {
						Code: code,
						Wrap: getter( mod.wrap ),
						ToU: getter( mod.tou || mod.readme, defs.tou ).parseJS(defs.subs),
						State: JSON.stringify(mod.state || mod.context || mod.ctx || {})						
					};

				Log("PUBLISH", name, `${from}=>${to}` );

				if ( from == to )  // use code as-is
					sql.query( 
						"INSERT INTO app.engines SET ? ON DUPLICATE KEY UPDATE ?", [ Copy(rev, {
							Name: name,
							Type: type,
							Enabled: 1
						}), rev ]);
				
				else  // convert code to requested type
					//CP.execFile("python", ["matlabtopython.py", "smop", fromFile, "-o", toFile], function (err) {
					FS.writeFile( fromFile, code, "utf8", (err) => {
						CP.exec( `sh ${from}to${to}.sh ${fromFile} ${toFile}`, (err, out) => {
							if (!err) 
								FS.readFile( toFile, "utf8", function (err,code) {
									rev.Code = code;
									if (!err)
										sql.query( 
											"INSERT INTO app.engines SET ? ON DUPLICATE KEY UPDATE ?", [ Copy(rev, {
												Name: name,
												Type: type,
												Enabled: 1
											}), rev ]);
								});									
						});
					});	
			
			}
			
			CP.exec(`cd ${name}.distro; sh publish.sh ${name} ${product}`);
			//`cd ${path}; sh ${filename}.sh "${now}" "${ver}" "${product}"`, (err) => {
			/*
			FS.access( pathname+".distro", FS.F_OK, (err) => {
				if (!err) 
					CP.exec(`
cd ${name}.distro
curl "${site.urls.master}/${name}.tou" >README.md
git commit -am "revised ToU"
git push origin master
`, 
						(err, out) => {
							Log("DISTRO PUB", err);
					});
			}); */
		},
		
		genLicense: function (code, product, type, secret, cb) {
			
			//Log("gen lic", secret);
			if (secret)
				switch (type) {
					case "html":
						var minCode = HTMLMIN.minify(code, {
							removeAttributeQuotes: true
						});
						cb( minCode, CRYPTO.createHmac("sha256", secret).update(minCode).digest("hex") );
						break;

					case "js":
						//cb(null); break;
						var
							e6Tmp = "./temps/e6/" + product,
							e5Tmp = "./temps/e5/" + product;

						FS.writeFile(e6Tmp, code, "utf8", (err) => {
							CP.exec( `cd /local/babel/node_modules/; .bin/babel ${e6Tmp} -o ${e5Tmp} --presets es2015,latest`, (err,log) => {
								FS.readFile(e5Tmp, "utf8", (err,e5code) => {
									Log("jsmin>>>>", err);

									if (err)
										cb( null );

									else {
										var min = JSMIN.minify( e5code );

										if (min.error) 
											cb( null );

										else   
											cb( min.code, CRYPTO.createHmac("sha256", secret).update(min.code).digest("hex") );
									}
								});
							});
						});
						break;						

					case "py":
						// cb(null); break;
						// problematic with python code as -O obvuscator cant be reseeded
						var pyTmp = "./temps/" + product;

						FS.writeFile(pyTmp, code.replace(/\t/g,"  "), "utf8", (err) => {					
							CP.exec(`pyminifier -O ${pyTmp}`, (err,minCode) => {
								Log("pymin>>>>", err);
								
								if (err)
									cb(null);

								else
									cb( minCode, CRYPTO.createHmac("sha256", secret).update(minCode).digest("hex") );
							});
						});
						break;

					case "m":
					case "me":
						//cb(null); break;
						/*
						Could use Matlab's pcode generator - but only avail within matlab
								cd to MATLABROOT (avail w matlabroot cmd)
								matlab -nosplash -r script.m
								start matlab -nospash -nodesktop -minimize -r script.m -logfile tmp.log

						but, if not on a matlab machine, we need to enqueue this matlab script from another machine via curl to totem

						From a matlab machine, use mcc source, then use gcc (gcc -fpreprocessed -dD -E  -P source_code.c > source_code_comments_removed.c)
						to remove comments - but you're back to enqueue.

						Better option to use smop to convert matlab to python, then pyminify that.
						*/

						var 
							mTmp = "./temps/matsrc/" + product,
							pyTmp = "./temps/matout/" + product;

						FS.writeFile(mTmp, code.replace(/\t/g,"  "), "utf8", (err) => {
							CP.execFile("python", ["matlabtopython.py", "smop", mTmp, "-o", pyTmp], (err) => {	
								Log("matmin>>>>", err);
								
								if (err)
									cb( null );

								else
									FS.readFile( pyTmp, "utf8", (err,pyCode) => {
										if (err) 
											cb( null );

										else
											CP.exec(`pyminifier -O ${pyTmp}`, (err,minCode) => {
												if (err)
													cb(null);

												else
													cb( minCode, CRYPTO.createHmac("sha256", secret).update(minCode).digest("hex") );
											});										
									});									
							});
						});
						break;

					case "jade":
					default:
						//cb(null); break;
						cb( code, CRYPTO.createHmac("sha256", secret).update(code).digest("hex") );
				}
			
			else
				cb( code, CRYPTO.createHmac("sha256", type).update(code).digest("hex") );
		},

		publishPlugins: function ( sql ) { //< publish all plugin products
			var paths = FLEX.paths.publish;
			Each( paths, (type, path) => { 		// get plugin file types to publish	
				FLEX.indexer( path, (files) => {	// get plugin file names to publish
					files.forEach( (file) => {
						var
							product = file,
							parts = product.split("."),
							filetype = parts.pop(),
							filename = parts.pop();
						
						switch (filetype) {
							case "js":
								var 
									now = new Date(),
									ver = "vX";

								FLEX.publishPlugin(sql, filename, type, FLEX.licenseOnRestart);
								break;
								
							case "jade":
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
								});
								break;
								
						}
					});	
				});
			});
		},
		
		sendMail: sendMail,
		
		errors: {
			badRequest: new Error("bad/missing request parameter(s)"),
			noBody: new Error("no body keys"),
			noID: new Error("missing record ID"),
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
		
		paths: {
			status: "./public/shares/status.xlsx",
			logins: "./public/shares/logins",
			publish: {
				js: "./public/js/",
				py: "./public/py/",
				me: "./public/me/",
				m: "./public/m/",
				jade: "./public/jade/"
			},
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
		
		reroute: {  //< default table -> db.table translators
		},
		
		// CRUDE interface
		select: {ds: selectDS}, 
		delete: {ds: deleteDS}, 
		update: {ds: updateDS}, 
		insert: {ds: insertDS}, 
		execute: {}, 
	
		fetcher: () => {Trace("data fetcher not configured");},  //< data fetcher
		uploader: () => {Trace("file uploader not configured");},  //< file uploader
		emitter: () => {Trace("client emitter not configured");},  //< client syncer
		thread: () => {Trace("sql thread not configured");},  //< sql threader
		skinner: () => {Trace("site skinner not configured");},  //< site skinner

		/*
		runEngine: function (req,res) {  // run engine and callback res(ctx || null) with updated context ctx

			ATOM.select(req, function (ctx) {  // compile and step the engine
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
		}, */
		
		/*
		eachPlugin: function ( sql, group, cb ) {  // callback cb(eng,ctx) with each engine and its context meeting ctx where clause
			sql.eachTable( group, function (table) { 
				ATOM.getEngine( sql, group, table, function (eng) {
					if (eng) cb(eng);
				});
			});
		},*/
		
		/*
		eachUsecase: function ( sql, group, where, cb ) {  // callback cb(eng,ctx) with each engine and its context meeting ctx where clause
			FLEX.eachPlugin( sql, group, function (eng) {
				if (eng)
					FLEX.getContext( sql, group+"."+table, where, function (ctx) {
						if (ctx) cb( eng, ctx );
					});
			});
		},*/
		
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
		
		getContext: function ( sql, host, where, cb ) {  //< callback cb(ctx) with primed plugin context or cb(null) if error
			
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
			
			sql.forFirst("", "SELECT * FROM ?? WHERE least(?,1) LIMIT 1", [host,where], function (ctx) {
				
				if (ctx) {
					ctx.Host = host;
					if ( ctx.Config ) config(ctx.Config, ctx);

					sql.jsonKeys( host, [], function (keys) {  // parse json keys
						//Log("json keys", keys);
						keys.parseJSON(ctx);
						/*
						keys.each(function (n,key) {
							try { 
								ctx[key] = JSON.parse( ctx[key] || "null" ); 
							}

							catch (err) {
								ctx[key] = null; 
							}

						}); */
						
						cb(ctx);
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
			FLEX.getContext( req.sql, req.group+"."+req.table, req.query, function (ctx) {
				
				//Log("get ctx", ctx);				
				if (ctx) {
					Copy(ctx,req.query);
					//Log("plugin req query", req.query);
					
					if ( ctx.Pipe )  // let host chipping-regulating service run this engine
						res( ctx );

					else
					if ( viaAgent = FLEX.viaAgent )  // allow out-sourcing to agents if installed
						viaAgent(req, res);

					else  // in-source the plugin and save returned results
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

			else   // in-source
				//FLEX.runEngine(req, res);
				ATOM.select(req, res);
		},
		
		config: function (opts, cb) {
		/**
		@method config
		Configure module with spcified options, publish plugins under TYPE/PLUGIN.js, establish
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
							CP.exec(`echo -e "${opts.body||'FYI'}\n" | mail -r "${opts.from}" -s "${opts.subject}" ${opts.to}`, function (err) {
								cb( err );
								//Trace("MAIL "+ (err || opts.to) );
							});
						}
					};
			
			if (FLEX.thread)
			FLEX.thread( function (sql) {				
				READ.config(sql);			
				
				if (CLUSTER.isMaster)   					
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
				
				sql.release();
			});
			
			if (CLUSTER.isMaster) {
				
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
				
				if (email = FLEX.mailer.RX)
					if (email.PORT)
						email.TRAN = new IMAP({
							  user: email.USER,
							  password: email.PASS,
							  host: email.HOST,
							  port: email.PORT,
							  secure: true,
							  //debug: function (err) { console.warn(ME+">"+err); } ,
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
			
			if (cb) cb(null);
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
			group: `mysqldump ${login} ${req.group} >admins/db/${req.group}.sql`,
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

FLEX.select.baseline = function Xselect(req, res) {

	var 
		query = req.query,
		sql = req.sql,
		ex = {
			gitlogs: 'git log --reverse --pretty=format:"%h||%an||%ce||%ad||%s" > gitlog'
		};
	
	CP.exec(ex.gitlogs, function (err,log) {

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
	
	sql.forEach("DELETEFILE", "SELECT * FROM app.files WHERE least(?,1)", {
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
	
	sql.forEach("EXEFILE", "SELECT * FROM app.files WHERE least(?,1)", {
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
			
			res( recs.sample( function (rec) { 
				return rec.ID; 
			}) );
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
						subs.each(function (n,name) {
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
					FLEX.indexer( path+file, function (subs) {
						subs.each(function (n,sub) {
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
FLEX.select.ATOMS = function Xselect(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	sql.query("SELECT ID,engineinfo(Name,Type,Updated,Classif,length(Code),Period,Enabled,length(Special)) AS Name FROM engines WHERE least(?,1) ORDER BY Name",
		guardQuery(query,true), 
		function (err, recs) {
			res(err || recs);
	});
}
*/

FLEX.select.config = function Xselect(req, res) {
	var 
		sql = req.sql,
		ID = 0,
		info = [];
	
	CP.exec("df -h /dev/mapper/centos-root", function (err,dfout,dferr) {
	CP.exec("netstat -tns", function (err,nsout,nserr) {
	CP.exec("npm list", function (err,swconfig) {
		swconfig.split("\n").forEach( (sw) => {
			info.push({ID: ID++, Type: "sw", Config: sw, Classif: "(U)"});
		});
		
		var type="";
		nsout.split("\n").forEach( (ns) => {
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
		/*
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
		});  */
		
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
	var sql = req.sql, log = req.log, query = req.query, pocs = FLEX.site.pocs;

	Log(pocs);
	
	if ( pocs.admin )
		sendMail({
			to:  pocs.admin,
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

/*
FLEX.select.DigitalGlobe = function Xselect(req, res) {  
// Digital globe interface

	var sql = req.sql, log = req.log, query = req.query;
} */

FLEX.select.AlgorithmService = function Xselect(req, res) { 
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
FLEX.select.NCL = function Xselect(req, res) { 
// Reserved for NCL and ESS service alerts
	var sql = req.sql, log = req.log, query = req.query;
}

FLEX.select.ESS = function Xselect(req, res) { 
// Reserved for NCL and ESS service alerts
	var sql = req.sql, log = req.log, query = req.query;
}
*/

// Uploads/Stores file interface

FLEX.select.uploads = 
FLEX.select.stores = 
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

FLEX.update.stores = 
FLEX.update.uploads = 
FLEX.update.uploads = 
FLEX.insert.uploads = 
function Xupdate(req, res) {
	
	var 
		sql = req.sql, 
		query = req.query, 
		body = req.body,
		client = req.client,
		canvas = body.canvas || {objects:[]},
		attach = [],
		now = new Date(),
		image = body.image,
		area = req.table,
		files = image ? [{
			name: name, // + ( files.length ? "_"+files.length : ""), 
			size: image.length/8, 
			image: image
		}] : body.files || [],
		tags = Copy(query.tag || {}, {Location: query.location || "POINT(0 0)"});

	/*
	Log({
		q: query,
		b: body,
		f: files,
		a: area,
		l: geoloc
	});*/
					
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

	res(SUBMITTED);

	Each(files, function (n, file) {

		var 
			buf = new Buffer(file.data,"base64"),
			srcStream = new STREAM.Readable({  // source stream for event ingest
				objectMode: true,
				read: function () {  // return null if there are no more events
					this.push( buf );
					buf = null;
				}
			});

		Trace(`UPLOAD ${file.filename} INTO ${area} FOR ${client}`, sql);

		FLEX.uploader( client, srcStream, area+"/"+file.filename, tags, function (fileID) {

			if (false)
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
							Ingest_Size: file.size,
							Ingest_Tag: query.tag || ""
						}, geoloc, req.client, geoloc
					]);

			if (false)
			sql.query( // credit the client
				"UPDATE openv.profiles SET Credit=Credit+?,useDisk=useDisk+? WHERE ?", [ 
					1000, file.size, {Client: req.client} 
				]);

			if (false) //(file.image)
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
	});
	
	Log("flex done uploading");
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

// CRUDE interfaces
// PLUGIN usecase editors

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
		ATOM.free(req,res);
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
						ATOM.select(req, res);
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
	
	sql.forEach("SELECT * FROM tests", [], function (test) {
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
	
	sql.forEach("SELECT GPCs FROM collects WHERE GPCs != ''", [], function (collect) {
		sql.query("UPDATE collects SET ?", {
			RPCs: JSON.stringify( computeRPCs( JSON.parse(collect.GPCs) ) )
		});
	});	
	
	sql.forEach("SELECT * FROM collects WHERE not BVI", [], function (collect) {
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

//FLEX.execute.aspreqts = 
//FLEX.execute.ispreqts = 
FLEX.execute.issues = function Xexecute(req, res) {
	var sql = req.sql, log = req.log, query = req.query;
	
	res(SUBMITTED);
	statRepo(sql);	
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

function sendMail(opts, sql) {
	
	Trace(`MAIL ${opts.to} RE ${opts.subject}`, sql);

	opts.from = "totem@noreply.gov";
	opts.alternatives = [{
		contentType: 'text/html; charset="ISO-59-1"',
		contents: ""
	}];

	if (opts.to) 
		if ( email = send = FLEX.mailer.TX.TRAN )
			email.sendMail(opts, function (err) {
				//Trace("MAIL "+ (err || opts.to) );
			});
}

/*
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
		
		sql.query("INSERT INTO queues SET ? ON DUPLICATE KEY UPDATE Count=Count+1", {
			Made: new Date(),
			Searching: find.replace(/ /g,""),
			Tokens: req.table + DOT + req.flags.search,
			Returned: recs.length,
			Count: 1,
			Client: req.client
		});
}
*/
	
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

function selectDS(req,res) {
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		query = req.query,
		index = req.index;
	
	/*
	if ( filters = flags.filters )
		filters.forEach( function (filter) {
			query[filter.property] = filter.value;
		}); */

	sql.run( Copy( flags, {
		crud: req.action,
		from: FLEX.reroute[req.table] || (req.group + "." + req.table),
		where: query,
		index: index,
		having: {},
		client: req.client
	}), null, function (err,recs) {

		res( err || recs );

	});
}

function insertDS(req, res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		body = req.body,
		query = req.query;

	sql.run( Copy( flags, {
		crud: req.action,
		from: FLEX.reroute[req.table] || (req.group + "." + req.table),
		set: body,
		client: req.client
	}), FLEX.emitter, function (err,info) {

		//Log(info);
		res( err || info );

	});
	
}

function deleteDS(req, res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		query = req.query;

	if ( query.ID )
		sql.run( Copy( flags, {
			crud: req.action,
			from: FLEX.reroute[req.table] || (req.group + "." + req.table),
			where: query,
			client: req.client
		}), FLEX.emitter, function (err,info) {

			//Log(info);
			res( err || info );

		});
	
	else
		res( FLEX.errors.noID );
	
}

function updateDS(req, res) {
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		body = req.body,
		ds = req.table,
		query = req.query;

	//Log(req.action, query, body);
	
	if ( isEmpty(body) )
		res( FLEX.errors.noBody );
	
	else
	if ( query.ID )
		sql.run( Copy( flags, {
			crud: req.action,
			from: FLEX.reroute[req.table] || (req.group + "." + req.table),
			where: query,
			set: body,
			client: req.client
		}), FLEX.emitter, function (err,info) {

			//Log(info);
			res( err || info );

			if (true) {  // update change journal if enabled
				sql.hawk({Dataset:ds, Field:""});  // journal entry for the record itself
				if (false)   // journal entry for each record key being changed
					for (var key in body) { 		
						sql.hawk({Dataset:ds, Field:key});
						sql.hawk({Dataset:"", Field:key});
					}
			}
			
		});
	
	else
		res( FLEX.errors.noID );
	
}

/*
function queryDS(req, res) {
		
	var 
		sql = req.sql,							// sql connection
		flags = req.flags,
		query = req.query,
		body = Copy(query,req.body),
		emit = FLEX.emitter;

	delete body.ID;

	Log(req.action, query, body);
	
	sql.context({ds: {
		table:	FLEX.reroute[req.table] || (req.group + "." + req.table),
		where:	flags.where || query,
		res:	res,
		order:	(flags.sort||"").parseJSON(null),
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
*/

//============ misc 

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

FLEX.select.login = function(req,res) {
	var 
		sql = req.sql, 
		query = req.query,
		site = FLEX.site,
		url = site.urls.worker,
		nick = site.nick,
		nickref = nick.tag("a",{href:url}),
		client = req.client,
		user = userID(client),
		group = req.group,
		profile = req.profile,
		userHome = `/local/users/${user}`,
		logins = FLEX.paths.logins,
		sudoJoin = `echo "${ENV.ADMIN_PASS} " | sudo -S `,
		isp = {
			machine: "totem.west.ile.nga.ic.gov",
			admin: "totemadmin@coe.ic.gov",
			ps: "brian.d.james@coe.ic.gov",
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
						cc: isp.ps,
						subject: `${nick} login failed`,
						body: err + `This is an automatted request from ${nick}.  Please provide ${isp.ps} "sudo" for ${prep} on ${isp.machine}`
					});
				}

				else  {
					res( function () { return isp.remotein; } );
					sql.query("UPDATE openv.profiles SET ? WHERE ?", [{User: user}, {Client: client}]);
					sendMail({
						to: client,
						cc: [isp.ps,isp.admin].join(";"),
						subject: `${nick} login established`,
						body: notice
					});
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

FLEX.select.proctor = function (req,res) {  //< grade quiz results
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
					});
				}
				
		});
	
	});
}
		
FLEX.select.wms = function (req,res) {
	
	var 
		sql = req.sql,
		query = req.query,
		fetcher = FLEX.fetcher,
		src = query.src || "";
	
	switch (src) {
		case "dglobe":
		case "omar":
		case "ess":
		default:
	}
	
	res("ok");
	
	if ( url = ENV[`WMS_${src.toUpperCase()}`] ) 
		fetcher(url.tag("?", query), null ,function (rtn) {
			Log("wms stat", rtn);
		});
}

FLEX.select.wfs = function (req,res) {  //< Respond with ess-compatible image catalog to induce image-spoofing in the chipper.
	var 
		sql = req.sql,
		query = req.query,
		fetcher = FLEX.fetcher,
		site = FLEX.site,
		ring = query.ring || [],
		src = (query.src || "").toUpperCase();
	
	switch (src) {
		case "DGLOBE":
		case "OMAR":
		case "ESS":
		default:
			//query.geometryPolygon = JSON.stringify({rings: JSON.parse(query.ring)});  // ring being monitored
			query.geometryPolygon = JSON.stringify({rings: ring});  // ring being monitored
	}

	delete query.ring;
	
	if ( url = ENV[`WFS_${src}`] )
		fetcher( url.tag("?", query), null, function (cat) {  // query catalog for desired data channel

			if ( cat ) {
				switch ( src ) {  // normalize cat response to ess
					case "DGLOBE":
					case "OMAR":
					case "ESS":
						var
							res = ( cat.GetRecordsResponse || {SearchResults: {}} ).SearchResults,
							collects = res.DatasetSummary || [],
							sets = [];

						collects.each( function (n,collect) {  // pull image collects from each catalog entry

							//Log(collect);
							var 
								image = collect["Image-Product"].Image,
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
									imageID: image.ImageId.replace(/ /g,""),
											// "12NOV16220905063EA00000 270000EA530040"
									mode: image.SensorCode,
									bands: parseInt(image.BandCountQuantity),
									gsd: parseFloat(image.MeanGroundSpacingDistanceDim)*25.4e-3,
									wms: 
										urls.wms.replace(
											"?REQUEST=GetCapabilities&VERSION=1.3.0",
											"?request=GetMap&version=1.1.1") 

										+ "".tag("?", {
											width: aoi.lat.pixels,
											height: aoi.lon.pixels,
											srs: "epsg%3A4326",
											format: "image/jpeg"
										}).replace("?","&")
								});

						});
					}

				res( sets );
			}

			else
				res( null );
		});	
	
	else
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
			imageID: "debug",
					// "12NOV16220905063EA00000 270000EA530040"
			mode: "XX", //image.SensorCode,
			bands: 0, //parseInt(image.BandCountQuantity),
			gsd: 0, //parseFloat(image.MeanGroundSpacingDistanceDim)*25.4e-3,
			wms: site.urls.master+"/shares/images/spoof.jpg"			
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

}

FLEX.select.matlab = function (req,res) {
	var
		sql = req.sql,
		query = req.query;
	
	
	res("flushed");
		
}

FLEX.select.help = function (req,res) {
	var 
		sql = req.sql,
		query = req.query,
		from = query.from,
		site = FLEX.site;
	
	res("email submitted");
	switch (from) {
		case "pmo":
		case "asp":
		case "isp":
		case "swap":
	}
	
}

function isEmpty(opts) {
	for ( var key in opts ) return false;
	return true;
}

FLEX.select.filestats = function (req,res) {
	var 
		sql = req.sql,
		query = req.query;
	
	var q = sql.query(
		"SELECT stats.*,voxels.Point AS Location, voxels.Radius AS Radius, "
		+"app.link(files.Name,concat('/files.view',char(63),'name=',files.Name)) AS Source "
		+"FROM app.stats "
		+"LEFT JOIN app.files ON files.ID = stats.fileID "
		+"LEFT JOIN app.voxels ON voxels.ID = stats.voxelID",
		[], function (err,recs) {
			
		Log(err);
		res( err || recs );
	});
	Log(q.sql);	
}

/*
function sysAgent(req,res) {
	var 
		query = req.query,
		cb = ATOM.mw.cb[query.job];
	
	Log("AGENT", query);
	cb(0);
}
*/

function sysConfig(req,res) {
/**
@method sysConfig
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
*/
	function Guard(query, def) {
		for (var n in query) return query;
		return def;
	}
	
	var query = Guard(req.query,false);
	
	if (query)
		req.sql.query("UPDATE config SET ?", query, function (err) {
			res( err || "parameter set" );
		});
}

function sysCheckpt(req,res) {
/*
@method sysCheckpt
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
*/
	CP.exec('source maint.sh checkpoint "checkpoint"');
	res("Checkpointing database");
}

function sysStart(req, res) {
/*
@method sysStart
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
*/
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
/**
@method sysBIT
Totem(req,res) endpoint for builtin testing
@param {Object} req Totem request
@param {Function} res Totem response
*/
	var N = req.query.N || 20;
	var lambda = req.query.lambda || 2;
	
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
		}, t0, {	parms: {f1:f1.rand(),f2:f2.rand(),f3:f3.rand()}, 
					table: tables.rand(), 
					action: actions.rand(),
					client: users.rand()
				});
	}
}

function sysCodes(req,res) {
/**
@method sysCodes
@deprecated
Totem(req,res) endpoint to return html code for testing connection
@param {Object} req Totem request
@param {Function} res Totem response
*/
	res( HTTP.STATUS_CODES );
}

function sysKill(req,res) {
/*
@method sysKill
@deprecated
@param {Object} req Totem request
@param {Function} res Totem response
*/
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

FLEX.select.ping = function sysPing(req,res) {
/**
@method sysPing
Totem(req,res) endpoint to test client connection
@param {Object} req Totem request
@param {Function} res Totem response
*/
	res("hello "+req.client);			
}

FLEX.select.help = function sysHelp(req,res) {
/**
@method sysHelp
Totem(req,res) endpoint to return all sys endpoints
@param {Object} req Totem request
@param {Function} res Totem response
*/
	res(
		  "/ping.sys check client-server connectivity<br>"
		+ "/bit.sys built-in test with &N client connections at rate &lambda=events/s<br>"
		+ "/codes.sys returns http error codes<br>"
		+ "/alert.sys broadcast alert &msg to all clients<br>"
		+ "/stop.sys stops server with client alert &msg<br>"
	);
}

FLEX.execute.gitreadme = function(req,res) {
	res("git commiting and push");
}

function Trace(msg,sql) {
	TRACE.trace(msg,sql);
}

FLEX.execute.publish = function (req,res) {
	var
		sql = req.sql,
		client = req.client,
		query = req.query,
		product = query.product;
	
	if (product) 
		sql.query( "SELECT * FROM app.releases WHERE ? ORDER BY Published DESC LIMIT 1", {Product:product}, (pubs) => {
			
			if ( pub = pubs[0] ) {
				res( `Publishing product ${product}` );
				
				var 
					parts = pub.Ver.split("."),
					ver = pub.Ver = parts.concat(parseInt(parts.pop()) + 1).join("."),
					product = pub.Product || "",
					parts = product.split("."),
					type = parts.pop(),
					name = parts.pop();
				
				FLEX.publishPlugin( sql, name, type, true );
			}
			
			else
				res( `Product ${product} does not exist` );
			
		});
	
	else
		res( "missing product parameter" );	
}

FLEX.select.status = function (req,res) {
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
	
	READ.xlsx(sql, FLEX.paths.status, function (recs) {

		if (recs ) {			
			Each( recs[0], (key,val) => {  // remove cols outside this year
				if ( key.startsWith("_") )
					if ( !key.startsWith(_year) )
						recs.forEach( (rec) => delete rec[key] );
			});
				
			if (from) 	// remove cols before from-week
				for (var n=1,N=from; n<N; n++)  
					recs.forEach( (rec) => delete rec[ _year+"-"+n] );

			if (to)		// remove cols after to-week
				for (var n=to+1, N=99; n<N; n++)
					recs.forEach( (rec) => delete rec[ _year+"-"+n] );

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
			res( new Error("could not find status.xlsx") );
		
	});
	
}

FLEX.select.pubsites = function (req,res) {
	var 
		sql = req.sql,
		query = req.query,
		product = query.product || "",
		proxy = query.proxy || "",
		site = FLEX.site,
		licensePath = `${site.urls.worker}/${product}?endservice=`,
		proxyPath = `${site.urls.worker}/${product}?proxy=${proxy}`,
		rtns = [];
	
	sql.query(
		"SELECT Name,Path FROM app.lookups WHERE ?", {Ref: product}, (err,recs) => {
		
		recs.forEach( (rec) => {
			rtns.push( `<a href="${licensePath}${rec.Path}">${rec.Name}</a>` );
		});
		
		if (proxy)
			rtns.push( `<a href="${proxyPath}">other</a>` );
			
		//rtns.push( `<a href="${site.urls.worker}/lookups.view?Ref=${product}">add</a>` );
		
		res( rtns.join(", ") );
	});

}

// UNCLASSIFIED
