// UNCLASSIFIED

/**
 * @module SQL
 * @public
 * @requires base
 * @requires mysql
 * @requires enum
 * @requires os
 * @requires fs
 * @requires cluster
 * @requires child_process
 * 
 * Interface with mysql database and job service via a crude protocol.
 * */

var 											// nodejs bindings
	FS = require("fs"), 						// file system resources
	OS = require("os"), 						// operating system resources
	CP = require("child_process"), 				// Process spawning
	CLUSTER = require('cluster'); 				// Work cluster manager

var
	SQL = module.exports = 
		require("../base").config(
			require("../enum").config( 
				require("mysql") 
		));

var
	ENV = process.env, 							// external variables
	LIST = ",",									// list separator
	DOT = ".", 									// table.type etc separator
	LOCKS = {};									// database locks

SQL.RecID = "ID";

/**
 * @method config
 * 
 * Configure module with spcified options, then callback the
 * initializer.  
 * */
SQL.config = function (opts,cb) {
	
	SQL.copy(opts,SQL);
	
	if (cb) cb(SQL);
	
	if (SQL.thread)
		SQL.thread( function (sql) {
			
			console.trace("EXTENDING SQL CONNECTOR");
			SQL.extend(sql.constructor, {
				crude: Crude,
				//guard: Guard,
				//norm: Norm,
				//submit: Submit,
				//reply: Reply,
				select: Select,
				delete: Delete,
				update: Update,
				insert: Insert,
				execute: Execute,
				flatten: Flatten,
				spy: Spy,
				Each: Each
			});
		
			sql.release();
		});		
	
	return SQL;
};
		
/**
 * @private
 * 
 * Private functions
 * */		

/**
 * @method crude
 * Execute a query, log transaction stats, broadcast query event to other clients, then
 * use the callback to reply to this client.
 *
 * Uses the following req parameters:
 * 
 * 		flags	query flags
 * 		joins 	table joins
 * 
 * and req parameters:
 * 
 * 		sql 	connection thread
 * 		client 	that is making this query 
 * 		source 	target table for this query
 * 		action 	crude transaction (insert,update,delete,select,execute)
 * 		journal	switch to enable journalling this transacion to the jou database
 * 		log 	transaction log info
 *
 * @param {Object} req HTTP requests 
 * @param {Object} res HTTP response
 * @param {Function} cb callback (typically SQL.close) that replies to client
 */
function Crude(req,res) {
	
	// Establish request flags
	
	setFlags(req);
	
	// Get req parameters
	
	var 
		sql = this,								// sql connection
		log = req.log||{RecID:0}, 							// transaction log
		client = req.client,
		area = req.area,
		table = (area ? area+DOT : "") + req.table,
		action = req.action,  					// action requested
		flags = req.flags || {}, 				// get flags
		joins = req.joins || {}, 				// get left joins
		//journal = req.journal,
		//client = log.Client, 					// client making request
		//area = req.params.area, 				// DB table located in
		//table = ( area || SQL.DBTX[log.Table] || SQL.DB ) + DOT + log.Table,   // fully qualifed table name
		//VTL = SQL.APP[action][log.Table],		// associated Virtual Table	Logic (engine)
		lockID = `${table}.${log.RecID}`, 			// id of record lock
		lock = req.lock = LOCKS[lockID]; 			// record lock
			
	// Get query parameters
	
	var	
		//query = req.query,
		query = req.query, 
		set = req.set,
		body = req.body,
		builds = flags.build,
		track = flags.track,
		queue = flags.queue,
		journal = [ req.journal ? "jou." + table : "" , log.Event, table, {ID: log.RecID} ],
		locking = flags.lock ? true : false,
		trace = req.flags.trace 
			? function (q) {
				console.log(q.sql);
			}
			: function () {};			
		
		//trace = action.toUpperCase() + " " + table,	
		//parse = action.toUpperCase() + " " + table;

	function setFlags(req) { 	// set and remap flags and joins from request query and body
		var 
			FLAGS = SQL.FLAGS,
			strip = FLAGS.STRIP,
			prefix = FLAGS.PREFIX,
			lists = FLAGS.LISTS,
			jsons = FLAGS.JSONS,
			debug = FLAGS.DEBUG,
			ID = FLAGS.ID;
			
		var
			query = req.query || {},
			body = req.body ||{},
			flags = req.flags = {},
			joins = req.joins = {};
		
		if (debug)
			console.info({
				i: "before",
				a: req.action,
				q: query,
				b: body,
				f: flags
			});

		for (var n in query) 		// remove bogus query parameters and remap query flags and joins
			if ( n in strip ) 				// remove bogus
				delete query[n];
			else
			if (n.charAt(0) == prefix) {  	// remap flag
				flags[n.substr(1)] = query[n];
				delete query[n];
			}
			else {							// remap join
				var parts = n.split(DOT);
				if (parts.length>1) {
					joins[parts[0]] = n+"="+query[n];
					delete query[n];
				}
			}	

		for (var n in query) {		// unescape query parameters
			var parm = query[n] = unescape(query[n]);

			if ( parm.charAt(0) == "[") 
				try { query[n] = JSON.parse(parm); } 
				catch (err) { delete query[n]; }
		}
		
		for (var n in body) 		// remap body flags
			if (n.charAt(0) == prefix) {  
				flags[n.substr(1)] = body[n];
				delete body[n];
			}
		
		if (ID in body) {  			// remap body record ID
			query[ID] = body[ID];
			delete body[ID];
		}
		
		for (var n in flags) { 		// unescape flags
			var parm = unescape(flags[n]);
			
			if (n in lists) 
				flags[n] = parm ? parm.split(",") : "";
			else
			if (n in jsons)
				try { flags[n] = JSON.parse(parm); } 
				catch (err) { delete flags[n]; }
			else
				flags[n] = parm;
		}

		if (debug)
			console.info({
				i: "after",
				a: req.action,
				q: query,
				b: body,
				f: flags,
				j: joins
			});
			
		return(req);
	}
	
	// journalling crude
	
	function SELECT(req,res) {

		var 	
			//client	= req.client,
			//table 	= req.table,
			//query 	= req.query, 
			//flags 	= req.flags || {},
			//joins	= req.joins || {},
			sort 	= flags.sort,
			page 	= flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
			builds	= req.builds || "*",
			search 	= flags.search,
			
			pivot	= flags.pivot,
			browse	= flags.browse,
			tree	= flags.tree,
			
			builds = flags.build || "*",

			cmd = 
				"SELECT SQL_CALC_FOUND_ROWS "
				+ Builds( builds, search, flags )
				+ ( tree 	? Tree( tree, query, flags ) : "" )			
				+ ( browse 	? Browse( browse,  query, flags ) : "" )
				+ ( pivot 	? Pivot( pivot,  query, flags ) : "" )
				+ ( table 	? From( table, joins ) : "" )
				+ Where( query, flags ) 
				+ Having( flags.score )
				+ ( flags.group ? Group( flags.group ) : "" )
				+ ( sort 	? Order( sort ) : "" )
				+ ( page 	? Page( page ) : "" );
			
		trace( sql.query(
			cmd,
			[table, Guard(query,true), page], function (err, recs) {
							
				res(err || recs);

				if (search && !err) 
					this.spy(req,recs);
		}));

	}
		
	function DELETE(req,res) {
		
		var 
			//query = req.query, 
			//table = req.table,
			//journal = req.journal,
			//flags = req.flags,
			queue = flags._queue;

		var cmd = 
			"DELETE " 
			+ ( table ? From( table ) : "" )
			+ Where( query, flags );
		
		if (false) 		// queue support (enable if useful)
			sql.query("SELECT Name FROM ?? WHERE ?",[table,query])
			.on("result", function (rec) {
				sql.query("DELETE FROM queues WHERE least(?,1) AND Departed IS NULL", {
					Client: "system",
					Class: queue,
					Job: rec.Name
				});
			});

		sql.query( 			// attempt journal
			"INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", 
			journal)
			
		.on("end", function () {
			trace( sql.query( 		// delete
				cmd, [table, query, Guard(query,false)], function (err,info) {
					res(err || info);
			}));
		});
		
		//if (journal) 
		//else
		//	sql.query( 			// delete
		//		cmd, [table, query, Guard(query,false)], function (err,info) {
		//			res(err || info);
		//	}); 
	}	
	
	function UPDATE(req,res) {
		var 
			//query = req.query, 
			//table = req.table,
			//journal = req.journal,
			//flags = req.flags,
			queue = flags._queue,
			
			cmd = Guard(set,false)
				? "UPDATE ?? SET ? " + Where(Guard(query,false), flags)
				: "#UPDATE IGNORED";
			
		sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)  // attempt journal
		.on("end", function () {
			
			trace( sql.query(cmd, [table,set,query], function (err,info) { 	// update
				
				res( err || info );
				
				if (queue && set[queue])   						// queue support
					sql.query("SELECT Name table ?? WHERE ?",[req.table,query])
					.on("result", function (rec) {
						sql.query("UPDATE queues SET ? WHERE least(?,1) AND Departed IS NULL", [{
							Departed:new Date()
						}, {
							Client:"system",
							Class:queue,
							Job:rec.Name
						}]);
						
						sql.query("INSERT INTO queues SET ?", {
							Client:"system",
							Class:queue,
							Job:rec.Name,
							State:req.set[queue],
							Arrived:new Date(),
							Notes:rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
						});
					});
			
				//sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)
				//.on("end", function () {
				//	sql.query( 								// insert
				//		cmd, [table,set,query], function (err,info) {
				//			res(err || info);
				//	});
				//});
			}));

		});
		
	}
	
	function INSERT(req,res) {

		var 
			//table = req.table,
			//journal = req.journal,
			//flags = req.flags,
			//set = req.set,
			queue = flags._queue,
			
			cmd = Guard(set,false)
				? "INSERT INTO ?? SET ?" 
				: "INSERT INTO ?? () VALUES ()";

		trace( sql.query(  	// insert
			cmd, [table,set], function (err,info) {
			
			res(err || info);
			
			if (false && queue)  // queue support (can enable but empty job name results)
				sql.query("SELECT Name FROM ?? WHERE ?",[req.table,{ID:info.insertId}])
				.on("result", function (rec) {
					sql.query("INSERT INTO queues SET ?", {
						Client	: "system",
						Class	: queue,
						Job	: rec.Name,
						State	: set[queue],
						Arrived	: new Date(),
						Notes	: rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
					});	
				});
		}));	

	}
	
	function EXECUTE(req,res) {
		res( new Error("Execute Reserved") );
	}
	
	// Remap request flags if navigating folders

	if (file = flags.file) {
		var cmd = req.query.cmd;
		var init = req.query.init;
		var query = req.query = {
			NodeID: init 
				? SQL.NODENAV.ROOT 
				: (req.query.target || SQL.NODENAV.ROOT)
		};
		
		flags.browse = file;
		flags.file = cmd;
		delete flags.pivot;
	}
							
	console.trace(
		(locking?"LOCK ":"FOR ")
		+ `${client} `
		+ (CLUSTER.isMaster ? "" : `CORE ${CLUSTER.worker.id}`)
		+ `${action} ${table}`
		//+ parse.replace("SQL_CALC_FOUND_ROWS","").replace("??",table)
	);

	if (locking) 				// Execute query in a locked transaction thread
		switch (action) {
			case "select":

				SELECT({
					client: client,
					builds: builds,
					query: query,
					table: table,
					flags: flags
				}, function (recs) {

					if (recs.constructor == Error) 
						res( recs );
					else
					if (rec = recs[0]) {
						var lockID = table+DOT+rec.ID,			// record lock name	
							lock = req.lock = LOCKS[lockID]; 	// record lock

						if (lock) {
							if (false && SQL.notify)
								SQL.notify( "update",  {
									table: lock.Table,
									body: {_Locked: true},
									ID: lock.RecID,
									client: lock.Client,
									flag: flags.client 
								});

							if (lock.Client == client) { 		// client owns this transaction

								lock.sql.query("COMMIT");
								delete LOCKS[lockID];
								req.lock = null;
								
								res(recs);
							}
							else {					// client does not own this transaction
								lock.Hits++;
								res(new Error("locked by "+lock.Client));
							}
						}
						else 
							SQL.thread( function (txsql) {  	// reserve this thread for as long as it lives

								var lock = req.lock = LOCKS[lockID] = { 	// create a lock
									Table: table,
									RecID: rec.ID,
									Hits: 0,
									sql	: txsql,
									Client: client
								};

								if (false && SQL.notify)     // may prove useful to enable
									SQL.notify( "update",  {
										table: lock.Table,
										body: {_Locked: false},
										ID: lock.RecID,
										client: lock.Client,
										flag: flags.client 
									});

								txsql.query("START TRANSACTION", function (err) {  // queue this transaction

									res(recs);
									
								});
								
								// dont release txsql - timer being used
								// to t/o if the client locks the record too long
							});
					}
					else
						res(new Error("empty"));					
				});
						
				break;
			
			case "delete":
				
				DELETE({
					table: table,
					query: query,
					flags: flags,
					journal: journal
				}, res);
				break;
										
			case "insert":

				if (lock) 
					if (lock.Client == client) {
						INSERT({
							table: table,
							query: query,
							flags: flags,
							set: body,
							journal: journal
						}, res);		

						lock.sql.query("COMMIT", function (err) {
							req.lock = null;
							delete LOCKS[lockID];
						});
					}
					else {
						lock.Hits++;
						res(new Error(lock.Client+" locked"));						
					}
				else 
					INSERT({
						table: table,
						query: query,
						flags: flags,
						set: body,
						journal: journal
					}, res);

				break;
				
			case "update":

				if (lock) 
					if (lock.Client == client) {
						UPDATE({
							table: table,
							query: query,
							flags: flags,
							set: body,
							journal: journal
						}, res);		

						lock.sql.query("COMMIT", function (err) {
							req.lock = null;
							delete LOCKS[lockID];
						});
					}
					else {
						lock.Hits++;
						res(new Error(lock.Client+" locked"));						
					}
				else 
					UPDATE({
						table: table,
						query: query,
						flags: flags,
						set: body,
						journal: journal
					}, res);

				break;
				
			case "execute":
				
				res( new Error("cant lock") );
				break;
				
			default:
			
				res( new Error("invalid request") );
				break;
		}
	
	else 						// Execute non-locking query
		switch (action) {
			case "select": 		// No journalling needed

				SELECT({
					client: client,
					builds: builds,
					query: query,
					table: table,
					flags: flags,
					joins: joins
				}, res);

				break;

			case "insert": 		// No journalling needed
				
				INSERT({
					table: table,
					query: query,
					flags: flags,
					set: body,
					journal: journal
				}, res);

				break;

			case "delete": 		// Journalling attempted
				
				DELETE({
					table: table,
					query: query,
					flags: flags,
					journal: journal
				}, res);

				break;

			case "update":		// Journalling attempted
			
				UPDATE({
					table: table,
					query: query,
					flags: flags,
					set: body,
					journal: journal
				}, res);

				break;

			case "execute": 	// Create-report table schema
				
				EXECUTE({
					table: table,
					query: query,
					flags: flags,
					set: body,
					journal: journal
				}, res);
				
				break;

			default:
				
				res(new Error("invalid request"));
				break;
				
		}	
	
	/*
	else  						// Execute non-locking query
		sql.query("SELECT *,count(ID) AS Found from engines WHERE least(?) LIMIT 0,1", {	// Query engine/table
			Name: log.Table, 
			Enabled: true, 
			Period: 0
		})
		.on("error", function (err) {
			res(err);
		})
		.on("result", function (eng) {  // Get Virtual Table engine

			if (eng.Found) {
				VTL = SQL.APP[action][eng.Name] = SQL.APP.ENGINE[action];
				
				if (VTL) {
					console.log("REGISTER "+eng.Engine+" ENGINE "+eng.Name);
					VTL(req, res);
				}
			}
			else 						// Query table (attempt journalling)
				switch (action) {
					case "select": 		// No journalling needed

						trace = sql.select({
							client: client,
							builds: builds,
							query: query,
							table: table,
							flags: flags,
							joins: joins
						}, res);

						break;

					case "insert": 		// No journalling needed
						
						trace = sql.insert({
							table: table,
							query: query,
							flags: flags,
							set: body,
							journal: journal
						}, res);

						break;

					case "delete": 		// Journalling attempted
						
						trace = sql.delete({
							table: table,
							query: query,
							flags: flags,
							journal: journal
						}, res);

						break;

					case "update":		// Journalling attempted
					
						trace = sql.update({
							table: table,
							query: query,
							flags: flags,
							set: body,
							journal: journal
						}, res);

						break;

					case "execute": 	// Create-report table schema
						
						trace = sql.execute({
							table: table,
							query: query,
							flags: flags,
							set: body,
							journal: journal
						}, res);
						
						break;

					default:
						
						res(new Error("invalid request"));
						break;
						
				}
		
		});
	*/

	// Notify clients of change.  Send originating client's client ID so client cant ignore its own changes.  

	//if (!SQL.APP[log.Action][log.Table])  	// Dont broadcast SQL.APP calls
	if (false)
		if (SQL.notify) 					// Clients are to be synched
			SQL.notify( log.Action, { 		// Broadcast change to clients
				table: log.Table, 
				body: body, 
				ID: log.RecID, 
				client: log.Client, 
				flag: flags.client
			});

}

/**
 * @method norm
 * Return an appropriate response to the callback for the given recs sets: data Array per 
 * associated req.flags, reply String, fault Error, data hash Object, or null.
 * 
 * @param {Object} req HTTP request.
 * @param {Object} res HTTP response.
 * @param {Array} recs SQL data or status.
 * @param {Function} cb callback(Object).
 */
function _Norm(req,res,recs,cb) {	
	var 
		sql = this,
		log = req.log,
		query = req.query,
		body = req.body,
		flags = req.flags,
		ok = flags._lock ? (req.lock ? "locked" : "unlocked") : "ok";

	if (recs)
		switch (recs.constructor) {
			case String:
				cb({  
					success: true,
					msg: recs,
					count: 0,
					data: []
				}); 
				break;
				
			case Error:
				cb({  
					success: false,
					msg: recs+"",  
					count: 0,
					data: []
				}); 
				break;
				
			case Object:
				cb({  
					success: true,
					msg: ok,  
					count: 0,
					data: recs
				}); 				
				break;
			
			case Array:
			
				if (flag = flags.jade) { 			// jadeify records  

					var	framework = flag.shift() || "extjs",
						rows = "";

					recs.each( function (n, rec) {

						var cols = "";
						flag.each( function (idx, jade) {
							if ( rec[jade] ) cols += rec[jade].indent("#fit") + "\n";
						});

						rows += cols + "\n";
					});
					
					var jade = 
							"extends LAYOUT\nappend LAYOUT.body\n".replace(/LAYOUT/g,framework)
							+ rows.indent("#table",{dims:"'[800,400]'"}).indent("");

//console.log(jade);
					htmlgen = SQL.RENDER.compile(jade, req) || function (req) { return "Bad Jade"; }; 

					cb( htmlgen(req) );
				}
				
				else
				if (flag = flags.tree)  			// treeify records  
					sql.query("select found_rows()")
					.on('result', function (stat) {
						cb({
							success: true,
							msg: ok,
							count: stat["found_rows()"],
							data: recs.treeify(0,recs.length,0,flag,"size")
						});
					});
					
				else
				if (flag = flags.json) {			// parse specified field 
					recs.each( function (n,rec) {
						flag.each( function (i,n) {
							try {
								rec[n] = JSON.parse(rec[n]);
							}
							catch (err) {
							}
						});
					});
					cb(recs);
				}
				
				else
				if (flag = flags.index) {     		// index records
					
					var group = flag[2],
						x = flag[0],
						y = flag[1];
					
					var rtn = group ? {} : {group: []};
					recs.each( function (n,rec) {
						var xy = group ? rtn[rec[group]] : rtn.group;
						if (!xy) xy = rtn[rec[group]] = [];
						xy.push([rec[x], rec[y]]);
					});
					
					cb({  
						success: true,
						msg: ok,
						count: 1,
						data: [rtn]
					});
					
					/*
						try {
							recs.each( function (n,rec) {
								var pts = new Array(flag.length);
								flag.each( function (i,idx) {
									pts[i] = rec[idx];
								});
								recs[n] = pts;
							});
						}
						catch (err) {}

						cb({  
							success: true,
							msg: ok,
							count: recs.length,
							data: recs
						});
					*/
				}

				/*else
				if (flag = flags.pair) {		// pair records
					var rtn = {};
					recs.each( function (n,rec) {
						var xy = rtn[flag[2]];
						if (!xy) xy = rtn[flag[2]] = [];
						xy.push([rec[flag[0]], rec[flag[1]]]);
					});
					
					cb({  
						success: true,
						msg: ok,
						count: 1,
						data: [rtn]
					});
				}*/

				else
				if (flag = flags.file) { 			// navigate records via pivot folders

					var browse = flags.browse; 
						Root   = flags.NodeID == SQL.NODENAV.ROOT,
						Parent = !Root ? flags.NodeID : "$",  // Parent hash must be kept nonempty
						Nodes  = !Root ? flags.NodeID.split(LIST) : [],					
						Folder = (Nodes.length<browse.length) ? browse[Nodes.length] : "",
						Files  = [];

console.log("NAVIGATE Recs="+recs.length+" NodeID="+flags.NodeID+" Nodes="+Nodes+" browse="+browse+" Folder="+Folder+" Client="+Parent+" Flag="+flag);
					
					if (Folder) {   	// at branch
						Files.push({	// prime the side tree area
							mime:"directory",
							ts:1334071677,
							read:1,
							write:0,
							size:999,
							hash: Parent,
							volumeid:"tbd",
							//phash: Back,	// cant do this for some reason
							name: Folder,
							locked:1,
							dirs:1
						});
					
						recs.each( function (n,rec) {
//console.log("rec "+n+" path="+rec.NodeID+" name="+Folder+":"+rec[Folder]);
							
							Files.push({
								mime: "directory",	// mime type
								ts:1310252178,		// time stamp format?
								read:1,				// read state
								write:0,			// write state
								size:666,			// size
								hash:rec.NodeID,	// hash name
								name:Folder+":"+rec[Folder], // flag name
								phash:Parent, 		// parent hash name
								locked:0,			// lock state
								volumeid:"tbd",
								dirs: 1 			// place in side tree too
							});
						});
					}
					else {				// at leaf
						Files.push({	// prime the side tree area
							mime:"directory",
							ts:1334071677,
							read:1,
							write:0,
							size:999,
							hash: Parent,
							volumeid:"tbd",
							//phash: Back,	// cant do this for some reason
							name: "Name",
							locked:1,
							dirs:1
						});

						recs.each( function (n,rec) {  // at leafs
							Files.push({
								mime: "application/tbd", //"application/x-genesis-rom",	//"image/jpg", // mime type
								ts:1310252178,		// time stamp format?
								read:1,				// read state
								write:0,			// write state
								size:111,			// size
								hash:rec.NodeID,		// hash name
								name:rec.Name || ("record "+n),			// flag name
								phash:Parent,		// parent hash name
								volumeid:"tbd",
								locked:0			// lock state
							});						
						});
					}
					
					switch (flag) {  	// Handle flag nav
						case "test":	// canonical test case for debugging					
							cb({  //root -> "l1_Lw"
								"cwd": { 
									"mime":"directory",
									"ts":1334071677,
									"read":1,
									"write":0,
									"size":0,
									"hash": "root",
									"volumeid":"l1_",
									"name":"Demo",
									"locked":1,
									"dirs":1},
									
								/*"options":{
									"path":"", //"Demo",
									"url":"", //"http:\/\/elfinder.org\/files\/demo\/",
									"tmbUrl":"", //"http:\/\/elfinder.org\/files\/demo\/.tmb\/",
									"disabled":["extract"],
									"separator":"\/",
									"copyOverwrite":1,
									"archivers": {
										"create":["application\/x-tar", "application\/x-gzip"],
										"extract":[] }
								},*/
								
								"files": [
									{
									"mime":"directory",
									"ts":1334071677,
									"read":1,
									"write":0,
									"size":0,
									"hash":"root",
									"volumeid":"l1_",
									"name":"Junk", //"Demo",
									"locked":1,
									"dirs":1},
								
									/*{
									"mime":"directory",
									"ts":1334071677,
									"read":1,
									"write":0,
									"size":0,
									"hash":"root",
									"volumeid":"l1_",
									"name":"Demo",
									"locked":1,
									"dirs":1},*/
									
									{
									"mime":"directory",
									"ts":1340114567,
									"read":0,
									"write":0,
									"size":0,
									"hash":"l1_QmFja3Vw",
									"name":"Backup",
									"phash":"root",
									"locked":1},
									
									{
									"mime":"directory",
									"ts":1310252178,
									"read":1,
									"write":0,
									"size":0,
									"hash":"l1_SW1hZ2Vz",
									"name":"Images",
									"phash":"root",
									"locked":1},
									
									{
									"mime":"directory",
									"ts":1310250758,
									"read":1,
									"write":0,
									"size":0,
									"hash":"l1_TUlNRS10eXBlcw",
									"name":"MIME-types",
									"phash":"root",
									"locked":1},
									
									{
									"mime":"directory",
									"ts":1268269762,
									"read":1,
									"write":0,
									"size":0,
									"hash":"l1_V2VsY29tZQ",
									"name":"Welcome",
									"phash":"root",
									"locked":1,
									"dirs":1},
									
									{
									"mime":"directory",
									"ts":1390785037,
									"read":1,
									"write":1,
									"size":0,
									"hash":"l2_Lw",
									"volumeid":"l2_",
									"name":"Test here",
									"locked":1},
									
									{
									"mime":"application\/x-genesis-rom",
									"ts":1310347586,"read":1,
									"write":0,
									"size":3683,
									"hash":"l1_UkVBRE1FLm1k",
									"name":"README.md",
									"phash":"root",
									"locked":1}
								],
								
								"api":"2.0","uplMaxSize":"16M","netDrivers":[],
								
								"debug":{
									"connector":"php",
									"phpver":"5.3.26-1~dotdeb.0",
									"time":0.016080856323242,
									"memory":"1307Kb \/ 1173Kb \/ 128M",
									"upload":"",
									"volumes":[
										{	"id":"l1_",
											"name":"localfilesystem",
											"mimeDetect":"internal",
											"imgLib":"imagick"},
								
										{	"id":"l2_",
											"name":"localfilesystem",
											"mimeDetect":"internal",
											"imgLib":"gd"}],
								
									"mountErrors":[]}
							});
							break;
							
						case "tree": 	// not sure when requested
							cb({
								tree: Files,

								debug: {
									connector:"php",
									phpver:"5.3.26-1~dotdeb.0",
									time:0.016080856323242,
									memory:"1307Kb \/ 1173Kb \/ 128M",
									upload:"",
									volumes:[{	id:"l1_",
												name:"localfilesystem",
												mimeDetect:"internal",
												imgLib:"imagick"},

											{	id:"l2_",
												name:"localfilesystem",
												mimeDetect:"internal",
												imgLib:"gd"}],

									mountErrors:[]
								}		
							});	
							break;
							
						case "size": 	// on directory info
							cb({
								size: 222
							});
							break;
							
						case "parents": // not sure when requested
						case "rename":  // on rename with name=newname
						case "flag": 	// on open via put, on download=1 via get
							cb({
								message: "TBD"
							});
							break;
						
						case "open":	// on double-click to follow			
							cb({
								cwd: Files[0], /*{ 
									mime:"directory",
									ts:1334071677,
									read:1,
									write:0,
									size:999,
									hash: flags.NodeID,
									phash: "", //cwdBack,
									volumeid:"tbd", //"l1_",
									name: Folder,
									locked:0,
									dirs:1},*/
									
								options: {
									path:"/", //cwdPath,
									url:"/", //"/root/",
									tmbUrl:"/root/.tmb/",
									disabled:["extract"],
									separator:"/",
									copyOverwrite:1,
									archivers: {
										create:["application/x-tar", "application/x-gzip"],
										extract:[] }
								},
								
								files: Files,
								
								api:"2.0",
								uplMaxSize:"16M",
								netDrivers:[],

								debug: {
									connector:"php",
									phpver:"5.3.26-1~dotdeb.0",
									time:0.016080856323242,
									memory:"1307Kb \/ 1173Kb \/ 128M",
									upload:"",
									volumes:[{	id:"l1_",
												name:"localfilesystem",
												mimeDetect:"internal",
												imgLib:"imagick"},

											{	id:"l2_",
												name:"localfilesystem",
												mimeDetect:"internal",
												imgLib:"gd"}],

									mountErrors:[]
								}		
							});
							break;
							
						default:
							cb({
								message: "bad flag navigation command"
							});
					}
				}
					
				else
				if (flag = flags.encap) {  			// encapsulate records into a hash
					var encap = {};
					encap[flag] = recs;
					cb(encap);
				}

				else
				if (SQL.APP[log.Action][log.Table])	// Return number of records returned by virtual table
					cb({  
						success: true,
						msg: ok,
						count: recs.length,
						data: recs
					});
				
				/*else
				if (flag = flags.pivot)
					sql.query("select found_rows()")
					.on('result', function (stat) {
						cb({  
							success: true,
							msg: ok,
							count: stat["found_rows()"],
							data: recs
						});
					});*/
					
				else 									// Return number of records scanned in table
					sql.query("select found_rows()")
					.on('result', function (stat) {
						cb({  
							success: true,
							msg: ok,
							count: stat["found_rows()"],
							data: recs
						});
					});

				// Notify clients of change.  Send originating client's client ID so client cant ignore its own changes.  

				if (!SQL.APP[log.Action][log.Table])  		// Dont broadcast SQL.APP calls
					if (SQL.notify) 					// Clients are to be synched
						SQL.notify( log.Action, { 		// Broadcast change to clients
							table: log.Table, 
							body: body, 
							ID: log.RecID, 
							client: log.Client, 
							flag: flags.client
						});

				break;
				
			default:

				cb({  
					success: true,
					msg: ok,
					count: 0,
					data: recs
				}); 

				break;
			
		}
	else
		cb({  
			success: true,
			msg: ok,
			count: 0,
			ID: 0,
			data: []
		}); 
}

/**
 * @method submit
 * Submit the crude query define by req.req (see SQLCON.crude), then reply to the client 
 * with a normalized response hash when the query completes.
 *
 * @param {Object} req HTTP requests 
 * @param {Object} res HTTP response
 * @param {Function} cb callback(rtn,cb)
 */

/*
function Submit(req,res,cb) {
	var sql = this;

	sql.crude(req, res, function (recs) { 			// start CRUD operation		
		sql.norm(req, res, recs, function (rtn) {	// return normalized response
			if (cb)
				cb(rtn, function (rtn) {			// modulate results
					sql.reply(res,rtn);				// then return to client
				});
			else
				sql.reply(res, rtn);				// return results to client
		});
	});
}
*/

/**
 * @method reply
 * Send normalized {success,msg,count,data} hash to the client and
 * terminate the sql connection.
 * 
 * @param {Object} res HTTP response
 * @param {Object} rtn hash to send
 */	
/*
function Reply(res,rtn) {
	var sql = this;
	
	res.send(rtn);
	sql.release();
}*/

// CRUDE interface

function Select(req,res) {

	this.crude(req, function (recs) {
		
		var
			flags = req.flags;
		
		if (flag = flags.jade) { 			// jadeify records  

			var	framework = flag.shift() || "extjs",
				rows = "";

			recs.each( function (n, rec) {

				var cols = "";
				flag.each( function (idx, jade) {
					if ( rec[jade] ) cols += rec[jade].indent("#fit") + "\n";
				});

				rows += cols + "\n";
			});
			
			var jade = 
`extends ${framework}
append ${framework}.body`
				+ rows.indent("#table",{dims:"'[800,400]'"}).indent("");

//console.log(jade);
			gen = SQL.RENDER.compile(jade, req);
			
			res( gen ? gen(req) : "Bad Jade");
		}
		
		else
		if (flag = flags.tree)  			// treeify records 
			res( recs.treeify(0,recs.length,0,flag,"size") );
			
		else
		if (flag = flags.json) {			// parse specified field 
			recs.each( function (n,rec) {
				flag.each( function (i,n) {
					try {
						rec[n] = JSON.parse(rec[n]);
					}
					catch (err) {
					}
				});
			});
			res(recs);
		}
		
		else
		if (flag = flags.index) {     		// index records
			
			var group = flag[2],
				x = flag[0],
				y = flag[1];
			
			var rtn = group ? {} : {group: []};
			recs.each( function (n,rec) {
				var xy = group ? rtn[rec[group]] : rtn.group;
				if (!xy) xy = rtn[rec[group]] = [];
				xy.push([rec[x], rec[y]]);
			});
			
			res( [rtn] );			
		}

		/*
		else
		if (flag = flags.pair) {		// pair records
			var rtn = {};
			recs.each( function (n,rec) {
				var xy = rtn[flag[2]];
				if (!xy) xy = rtn[flag[2]] = [];
				xy.push([rec[flag[0]], rec[flag[1]]]);
			});
			
			res({  
				success: true,
				msg: ok,
				count: 1,
				data: [rtn]
			});
		}*/

		else
		if (flag = flags.file) { 			// navigate records via pivot folders

			var browse = flags.browse; 
				Root   = flags.NodeID == SQL.NODENAV.ROOT,
				Parent = !Root ? flags.NodeID : "$",  // Parent hash must be kept nonempty
				Nodes  = !Root ? flags.NodeID.split(LIST) : [],					
				Folder = (Nodes.length<browse.length) ? browse[Nodes.length] : "",
				Files  = [];

console.log("NAVIGATE Recs="+recs.length+" NodeID="+flags.NodeID+" Nodes="+Nodes+" browse="+browse+" Folder="+Folder+" Client="+Parent+" Flag="+flag);
			
			if (Folder) {   	// at branch
				Files.push({	// prime the side tree area
					mime:"directory",
					ts:1334071677,
					read:1,
					write:0,
					size:999,
					hash: Parent,
					volumeid:"tbd",
					//phash: Back,	// cant do this for some reason
					name: Folder,
					locked:1,
					dirs:1
				});
			
				recs.each( function (n,rec) {
//console.log("rec "+n+" path="+rec.NodeID+" name="+Folder+":"+rec[Folder]);
					
					Files.push({
						mime: "directory",	// mime type
						ts:1310252178,		// time stamp format?
						read:1,				// read state
						write:0,			// write state
						size:666,			// size
						hash:rec.NodeID,	// hash name
						name:Folder+":"+rec[Folder], // flag name
						phash:Parent, 		// parent hash name
						locked:0,			// lock state
						volumeid:"tbd",
						dirs: 1 			// place in side tree too
					});
				});
			}
			else {				// at leaf
				Files.push({	// prime the side tree area
					mime:"directory",
					ts:1334071677,
					read:1,
					write:0,
					size:999,
					hash: Parent,
					volumeid:"tbd",
					//phash: Back,	// cant do this for some reason
					name: "Name",
					locked:1,
					dirs:1
				});

				recs.each( function (n,rec) {  // at leafs
					Files.push({
						mime: "application/tbd", //"application/x-genesis-rom",	//"image/jpg", // mime type
						ts:1310252178,		// time stamp format?
						read:1,				// read state
						write:0,			// write state
						size:111,			// size
						hash:rec.NodeID,		// hash name
						name:rec.Name || ("record "+n),			// flag name
						phash:Parent,		// parent hash name
						volumeid:"tbd",
						locked:0			// lock state
					});						
				});
			}
			
			switch (flag) {  	// Handle flag nav
				case "test":	// canonical test case for debugging					
					res({  //root -> "l1_Lw"
						"cwd": { 
							"mime":"directory",
							"ts":1334071677,
							"read":1,
							"write":0,
							"size":0,
							"hash": "root",
							"volumeid":"l1_",
							"name":"Demo",
							"locked":1,
							"dirs":1},
							
						/*"options":{
							"path":"", //"Demo",
							"url":"", //"http:\/\/elfinder.org\/files\/demo\/",
							"tmbUrl":"", //"http:\/\/elfinder.org\/files\/demo\/.tmb\/",
							"disabled":["extract"],
							"separator":"\/",
							"copyOverwrite":1,
							"archivers": {
								"create":["application\/x-tar", "application\/x-gzip"],
								"extract":[] }
						},*/
						
						"files": [
							{
							"mime":"directory",
							"ts":1334071677,
							"read":1,
							"write":0,
							"size":0,
							"hash":"root",
							"volumeid":"l1_",
							"name":"Junk", //"Demo",
							"locked":1,
							"dirs":1},
						
							/*{
							"mime":"directory",
							"ts":1334071677,
							"read":1,
							"write":0,
							"size":0,
							"hash":"root",
							"volumeid":"l1_",
							"name":"Demo",
							"locked":1,
							"dirs":1},*/
							
							{
							"mime":"directory",
							"ts":1340114567,
							"read":0,
							"write":0,
							"size":0,
							"hash":"l1_QmFja3Vw",
							"name":"Backup",
							"phash":"root",
							"locked":1},
							
							{
							"mime":"directory",
							"ts":1310252178,
							"read":1,
							"write":0,
							"size":0,
							"hash":"l1_SW1hZ2Vz",
							"name":"Images",
							"phash":"root",
							"locked":1},
							
							{
							"mime":"directory",
							"ts":1310250758,
							"read":1,
							"write":0,
							"size":0,
							"hash":"l1_TUlNRS10eXBlcw",
							"name":"MIME-types",
							"phash":"root",
							"locked":1},
							
							{
							"mime":"directory",
							"ts":1268269762,
							"read":1,
							"write":0,
							"size":0,
							"hash":"l1_V2VsY29tZQ",
							"name":"Welcome",
							"phash":"root",
							"locked":1,
							"dirs":1},
							
							{
							"mime":"directory",
							"ts":1390785037,
							"read":1,
							"write":1,
							"size":0,
							"hash":"l2_Lw",
							"volumeid":"l2_",
							"name":"Test here",
							"locked":1},
							
							{
							"mime":"application\/x-genesis-rom",
							"ts":1310347586,"read":1,
							"write":0,
							"size":3683,
							"hash":"l1_UkVBRE1FLm1k",
							"name":"README.md",
							"phash":"root",
							"locked":1}
						],
						
						"api":"2.0","uplMaxSize":"16M","netDrivers":[],
						
						"debug":{
							"connector":"php",
							"phpver":"5.3.26-1~dotdeb.0",
							"time":0.016080856323242,
							"memory":"1307Kb \/ 1173Kb \/ 128M",
							"upload":"",
							"volumes":[
								{	"id":"l1_",
									"name":"localfilesystem",
									"mimeDetect":"internal",
									"imgLib":"imagick"},
						
								{	"id":"l2_",
									"name":"localfilesystem",
									"mimeDetect":"internal",
									"imgLib":"gd"}],
						
							"mountErrors":[]}
					});
					break;
					
				case "tree": 	// not sure when requested
					res({
						tree: Files,

						debug: {
							connector:"php",
							phpver:"5.3.26-1~dotdeb.0",
							time:0.016080856323242,
							memory:"1307Kb \/ 1173Kb \/ 128M",
							upload:"",
							volumes:[{	id:"l1_",
										name:"localfilesystem",
										mimeDetect:"internal",
										imgLib:"imagick"},

									{	id:"l2_",
										name:"localfilesystem",
										mimeDetect:"internal",
										imgLib:"gd"}],

							mountErrors:[]
						}		
					});	
					break;
					
				case "size": 	// on directory info
					res({
						size: 222
					});
					break;
					
				case "parents": // not sure when requested
				case "rename":  // on rename with name=newname
				case "flag": 	// on open via put, on download=1 via get
					res({
						message: "TBD"
					});
					break;
				
				case "open":	// on double-click to follow			
					res({
						cwd: Files[0], /*{ 
							mime:"directory",
							ts:1334071677,
							read:1,
							write:0,
							size:999,
							hash: flags.NodeID,
							phash: "", //cwdBack,
							volumeid:"tbd", //"l1_",
							name: Folder,
							locked:0,
							dirs:1},*/
							
						options: {
							path:"/", //cwdPath,
							url:"/", //"/root/",
							tmbUrl:"/root/.tmb/",
							disabled:["extract"],
							separator:"/",
							copyOverwrite:1,
							archivers: {
								create:["application/x-tar", "application/x-gzip"],
								extract:[] }
						},
						
						files: Files,
						
						api:"2.0",
						uplMaxSize:"16M",
						netDrivers:[],

						debug: {
							connector:"php",
							phpver:"5.3.26-1~dotdeb.0",
							time:0.016080856323242,
							memory:"1307Kb \/ 1173Kb \/ 128M",
							upload:"",
							volumes:[{	id:"l1_",
										name:"localfilesystem",
										mimeDetect:"internal",
										imgLib:"imagick"},

									{	id:"l2_",
										name:"localfilesystem",
										mimeDetect:"internal",
										imgLib:"gd"}],

							mountErrors:[]
						}		
					});
					break;
					
				default:
					res({
						message: "bad flag navigation command"
					});
			}
		}
			
		else
		if (flag = flags.encap) {  			// encapsulate records into a hash
			var encap = {};
			encap[flag] = recs;
			res(encap);
		}

		/*
		else
		if (SQL.APP[log.Action][log.Table])	// Return number of records returned by virtual table
			res({  
				success: true,
				msg: ok,
				count: recs.length,
				data: recs
			});
		*/
		
		/*
		else
		if (flag = flags.pivot)
			sql.query("select found_rows()")
			.on('result', function (stat) {
				res({  
					success: true,
					msg: ok,
					count: stat["found_rows()"],
					data: recs
				});
			});*/
			
		else 								// Return number of records scanned in table
			res(recs);
		
	});
}

function Update(req,res) {	
	this.crude(req,res);
}
function Insert(req,res) {	
	this.crude(req,res);
}
function Delete(req,res) {	
	this.crude(req,res);
}
function Execute(req,res) {	
	this.crude(req,res);
}

/*
function Select(args,cb) {

	var 	
		sql 	= this,
		client	= args.client,
		query 	= args.query, 
		table 	= args.table,
		flags 	= args.flags || {},
		joins	= args.joins || {},
		sort 	= flags.sort,
		page 	= flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
		builds	= args.builds || "*",
		search 	= flags.search,
		
		pivot	= flags.pivot,
		browse	= flags.browse,
		tree	= flags.tree,
		
		builds = flags.build || "*",

		cmd = 
			"SELECT SQL_CALC_FOUND_ROWS "
			+ Builds( builds, search, flags )
			+ ( tree 	? Tree( tree, query, flags ) : "" )			
			+ ( browse 	? Browse( browse,  query, flags ) : "" )
			+ ( pivot 	? Pivot( pivot,  query, flags ) : "" )
			+ ( table 	? From( table, joins ) : "" )
			+ Where( query, flags ) 
			+ Having( flags.score )
			+ ( flags.group ? Group( flags.group ) : "" )
			+ ( sort 	? Order( sort ) : "" )
			+ ( page 	? Page( page ) : "" );
		
	var q = sql.query(
		cmd,
		[table, Guard(query,true), page], function (err, recs) {
						
			cb(err || recs);

			if (search && !err) 
				Spy(args,recs);
	});

	if (args.trace)  console.log(q.sql);
		
	return cmd;
}
*/

/*
function Delete(args,cb) {
	
	var 
		sql = this,
		query = args.query, 
		table = args.table,
		journal = args.journal,
		flags = args.flags,
		queue = flags._queue;

	var cmd = 
		"DELETE " 
		+ ( table ? From( table ) : "" )
		+ Where( query, flags );
	
	if (false) 		// queue support (enable if useful)
		sql.query("SELECT Name FROM ?? WHERE ?",[table,query])
		.on("result", function (rec) {
			sql.query("DELETE FROM queues WHERE least(?,1) AND Departed IS NULL", {
				Client: "system",
				Class: queue,
				Job: rec.Name
			});
		});

	sql.query( 			// attempt journal
		"INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", 
		journal)
		
	.on("end", function () {
		sql.query( 		// delete
			cmd, [table, query, Guard(query,false)], function (err,info) {
				cb(err || info);
		});
	});
	
	//if (journal) 
	//else
	//	sql.query( 			// delete
	//		cmd, [table, query, Guard(query,false)], function (err,info) {
	//			cb(err || info);
	//	}); 

	return cmd;
}
*/

/*
function Update(args,cb) {
	var 
		sql = this,
		query = args.query, 
		table = args.table,
		set = args.set,
		journal = args.journal,
		flags = args.flags,
		queue = flags._queue,
		
		cmd = Guard(set,false)
			? "UPDATE ?? SET ? " + Where(Guard(query,false), flags)
			: "#UPDATE IGNORED";
		
	sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)  // attempt journal
	.on("end", function () {
		
		sql.query(cmd, [table,set,query], function (err,info) { 	// update
			
			cb( err || info );
			
			if (queue && set[queue])   						// queue support
				sql.query("SELECT Name table ?? WHERE ?",[args.table,query])
				.on("result", function (rec) {
					sql.query("UPDATE queues SET ? WHERE least(?,1) AND Departed IS NULL", [{
						Departed:new Date()
					}, {
						Client:"system",
						Class:queue,
						Job:rec.Name
					}]);
					
					sql.query("INSERT INTO queues SET ?", {
						Client:"system",
						Class:queue,
						Job:rec.Name,
						State:args.set[queue],
						Arrived:new Date(),
						Notes:rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
					});
				});
		
			//sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)
			//.on("end", function () {
			//	sql.query( 								// insert
			//		cmd, [table,set,query], function (err,info) {
			//			cb(err || info);
			//	});
			//});
		});
		
	});
	
	return cmd;
}
*/

/*
function Insert(args,cb) {

	var 
		sql = this,
		table = args.table,
		set = args.set,
		journal = args.journal,
		flags = args.flags,
		queue = flags._queue,
		
		cmd = Guard(set,false)
			? "INSERT INTO ?? SET ?" 
			: "INSERT INTO ?? () VALUES ()";

	sql.query(  	// insert
		cmd, [table,set], function (err,info) {
		
		cb(err || info);
		
		if (false && queue)  // queue support (can enable but empty job name results)
			sql.query("SELECT Name FROM ?? WHERE ?",[args.table,{ID:info.insertId}])
			.on("result", function (rec) {
				sql.query("INSERT INTO queues SET ?", {
					Client	: "system",
					Class	: queue,
					Job	: rec.Name,
					State	: set[queue],
					Arrived	: new Date(),
					Notes	: rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
				});	
			});
	});	

	return cmd;
}
*/

/*
function Execute(args,cb) {

	var 
		sql = this,
		table = args.table,
		journal = args.journal,
		flags = args.flags,
		queue = flags._queue,		
		cmd = "EXECUTE "+table;

	if (false) {
		sql.query("CREATE ("+args.set+") ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8", {}, function (err,recs) {
			cb(err || recs);
		});
		sql.query(args.query.replace("REPORT","SHOW CREATE"), function (err,recs) {
			cb(err ? err : recs[0]["Create Table"].split("\n").slice(1,-1));
		});
	}
	else
		cb(new Error("unsupported action"));
	
	return cmd;
}
*/

/**
 * @method flatten
 * 
 * Flatten entire database for searching the catalog
 * */
function Flatten(flags, catalog, limits, cb) {
	
	function flatten( sql, rtns, depth, order, catalog, limits, returncb, matchcb) {
		var table = order[depth];
		
		if (table) {
			var match = catalog[table];
			var selects = matchcb(match);
			
			var qual = " using "+ (selects ? selects : "open")  + " search limit " + limits.records;
			
			console.log("CATALOG ("+table+qual+") HAS "+rtns.length);
		
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

function Guard(query, def) {
	for (var n in query) return query;
	return def;
}

function Spy(req,recs) {
	var sql = this,
		flags = req.flags || {};
	
	console.log("SPY ON "+req.client);
	
	if (find = flags.find)
		sql.query("INSERT INTO searches SET ? ON DUPLICATE KEY UPDATE Count=Count+1", {
			Made: new Date(),
			Searching: find.replace(/ /g,""),
			Tokens: req.table + DOT + req.flags.search,
			Returned: recs.length,
			Count: 1,
			Client: req.client
		});
}

function Each(query, args, cb) {
	
	if (cb)
		this.query(query, args)
		.on("result", cb)
		.on("error", function (err) {
		});
	else
		this.query(query,args);
}

function Search( flags ) {
	var	search = flags.search || "FullSearch";

	if ( find = flags.find = flags.has ) 
		return ( search.enclose("CONCAT") + "," + JSON.stringify(find) ).enclose("INSTR");

	else
	if ( find = flags.find = flags.pat ) 
		return search + " LIKE " + JSON.stringify(find);

	else
		return "";
}

function Page( page ) {
	return " LIMIT ?";
}

function Having( having ) {
	if (having)
		return " HAVING Score>"+having;
	else
		return "";
}

function Order( sorts ) {
	return " ORDER BY " + SQL.joinify( sorts, LIST, function (n,sort) {
		
		if (sort.constructor == Object) 
			return "`" + sort.property + "` " + sort.direction;
			
		else
			return sort;
			
	});
}

function Group( group ) { 
	return " GROUP BY " + group;
}

function Where(query, flags) {
	var search = Search(flags);
	var rtn = " WHERE least(?,1)";
	
	if (search) rtn += " AND "+search;

	SQL.each( query, function (n, arg) {
		if (arg.constructor == Array) {
			delete query[n];
			
			rtn += " AND " + n;
			
			switch (arg.length) {
				case 0:
					rtn += " IS NULL ";
					break;
				case 1:
					rtn += arg[0];
					break;							
				case 2: 
					rtn += " BETWEEN " + arg[0] + " AND " + arg[1];
					break;
				case 3:
				default:
			}
		}
	});
	
	return rtn;
}

function From(from, joins) {
	var rtn = [" FROM ??"];

	SQL.each( joins, function (join,on) {
		rtn.push( "LEFT JOIN "+join+" ON "+on );
	});
	
	return rtn.join(" ");
}

function Pivot(pivot, query, flags) {
	var	NodeID = query.NodeID || "root",
		nodes = (NodeID == "root") ? [] : NodeID.split(LIST);
	
	flags.group = nodes.length ? "" : pivot.join();

	if (flags.group) {  		// at the root
		var rtn = 
			  ", " + (pivot.join(SQL.NODENAV.JOIN).enclose("concat")+" as char").enclose("cast") + " as NodeID"
			+ ", " + SQL.RECID.enclose("count") + " as NodeCount"	
			+ ", false as leaf, true as expandable, false as expanded";
//			+ ", 'root' as parentId";
	}
	else {  					// requesting all nodes under a specific node
		var rtn = 
			  ", " + SQL.RECID + " as NodeID"
//			  ", " + SQL.RECID + " as ID"
			+ ", 1 as NodeCount"
			+ ", true as leaf, true as expandable, false as expanded";
//				+ ", '" + NodeID + "' as parentId";
		
		nodes.each( function (n,node) {
			query[ pivot[n] ] = node;
		});		
	}

	// Convert NodeID to a flag 

	flags.NodeID = NodeID;
	delete query.NodeID;
	
	return rtn;
}

function Tree(tree, query, flags) {
	flags.group = tree.join();
	return "";
}

function Browse(browse, query, flags) {
	var	NodeID = query.NodeID,	
		nodes = NodeID ? NodeID.split(LIST) : [];
	
	flags.group = (nodes.length<browse.length) ? browse[nodes.length] : "";

	if (flags.group) { 
		var rtn = 
			  ", " + (browse.slice(0,nodes.length+1).join(SQL.NODENAV.JOIN).enclose("concat")+" as char").enclose("cast") + " as NodeID"
			+ ", " + SQL.RECID.enclose("count") + " as NodeCount"	
			+ ", false as leaf, true as expandable, false as expanded";
			
		nodes.each( function (n,node) {
			query[ browse[n] ] = node;
		});
	}
	else  {
		var rtn = 
			  ", " + (browse.concat([SQL.RECID]).join(SQL.NODENAV.JOIN).enclose("concat")+" as char").enclose("cast") + " as NodeID"
			+ ", 1 as NodeCount"	
			+ ", true as leaf, false as expandable, true as expanded";
			
		nodes.each( function (n,node) {
			query[(n >= browse.length) ? SQL.RECID : browse[n]] = node;
		});
	}
	
	// Convert NodeID to a flag 
	
	flags.NodeID = NodeID;
	delete query.NodeID;
	
	return rtn;
}

function Builds(builds, search, flags) {
	var find = "";
	
	if (builds.constructor == String) builds = builds ? [ builds ] : [];
	
	if ( find = flags.find = flags.nlp ) {
		flags.score = flags.score || 0.1;		
		builds.push( search.enclose("MATCH") + " " + JSON.stringify(find).enclose("AGAINST") + " AS Score" );	
	}
	else
	if ( find = flags.find = flags.bin ) {
		flags.score = flags.score || 0.1;		
		builds.push( search.enclose("MATCH") + " " + (JSON.stringify(find)+" IN BOOLEAN MODE").enclose("AGAINST") + " AS Score" );
	}
	else
	if ( find = flags.find = flags.exp ) {
		flags.score = flags.score || 0.1;		
		builds.push( search.enclose("MATCH") + " " + (JSON.stringify(find)+" WITH QUERY EXPANSION").enclose("AGAINST") + " AS Score" );
	}

	return builds.join(LIST);

}

// Initialize

SQL.config({

	/**
	* Specifies database connection parameters for EXAPP.
	*/
	//DB: "none", 					// Default database
	notify: null,		 			// Emitter to sync clients
	RENDER : null, 					// Jade renderer
	APP : null,	 					// Default virtual table logic
	POOL : null, 					// No pool until SQB configured
	BIT : null,						// No BIT-mode until set to a SYNC hash
	TRACE : false,					// Trace SQL querys to the console
	//USER : ENV.DB_USER,				// SQL client account (safe login for production)
	//PASS : ENV.DB_PASS,				// Passphrase to the SQL DB 
	//SESSIONS : 2,					// Maxmimum number of simultaneous sessions
	DBTX : {						// Table to database translator
		issues: "openv",
		tta: "openv",
		standards: "openv",
		milestones: "openv",
		txstatus: "openv",
		apps: "openv",
		trades: "openv",
		hwreqts: "openv",
		options: "openv",
		swreqts: "openv",
		aspreqts: "openv",
		ispreqts: "openv" },
	RESET : 12,	 					// mysql connection pool timer (hours)
	RECID : "ID",					// DB key field
	NODENAV : {						// specs for folder navigation
		ROOT : "", 					// Root node ID
		JOIN : ",'"+LIST+"',"				// Node joiner (embed same slash value above)
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
	
	FLAGS: {						//< Properties for request flags
		STRIP:	 					//< Flags to strip from request
			{"":1, "_":1, leaf:1, _dc:1, id:1, "=":1, "?":1}, 		
		
		JSONS: {
			sort: 1,
			build: 1
		},
		
		LISTS: { 					//< Array list flags
			pivot: 1,
			browse: 1,
			index: 1,
			file: 1,
			tree: 1,
			jade: 1,
			json: 1
		},
		
		ID: "ID", 					//< SQL record ID
		PREFIX: "_",				//< Prefix that indicates a field is a flag
		DEBUG: true 				//< Echo flags before and after parse
	}

});

// UNCLASSIFIED
