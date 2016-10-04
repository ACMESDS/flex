// UNCLASSIFIED

/**
 * @module SQL
 * @public
 * @requires base
 * @requires mysql
 * @requires os
 * @requires fs
 * @requires cluster
 * @requires child_process
 * 
 * Interface with mysql database and job service via a crude protocol.
 * */
var 											// globals
	ENV = process.env, 							// external variables
	LIST = ",",									// list separator
	DOT = ".", 									// table.type etc separator
	LOCKS = {};									// database locks

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
		select: crude, //sqlCrude,
		delete: crude, //sqlCrude,
		update: crude, //sqlCrude,
		insert: crude, //sqlCrude,
		execute: crude, //sqlCrude,
	
		RECID: "ID", 					// Default unique record identifier
		emit: null,		 				// Emitter to sync clients
		thread: null, 					// SQL connection threader
		skin : null, 					// Jade renderer
		TRACE : true,					// Trace SQL querys to the console
		//BIT : null,					// No BIT-mode until set to a SYNC hash
		//POOL : null, 					// No pool until SQB configured
		//DB: "none", 					// Default database
		//APP : null,	 				// Default virtual table logic
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
					context, //$$$$
					crude, //$$$$
					flattenCatalog
				]);

				sql.release();
			});		
		}

};

/**
 * @method sqlCrude
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
/*
function sqlCrude(req,res) {
		
	var 
		sql = req.sql,							// sql connection
		log = req.log||{RecID:0}, 				// transaction log
		client = req.client, 					// client info
		area = req.area, 	
		table = SQL.DBTX[req.table] || req.table,
		action = req.action,  					// action requested				
		joins = req.joins || {}, 				// get left joins
		journal = [ req.journal ? "jou." + table : "" , log.Event, table, {ID: log.RecID} ],

		// query, body and flags parameters
		
		query = req.query, 
		body = req.body,
		flags = req.flags; //setFlags(req);
		
	var
		builds 	= flags.build,
		track 	= flags.track,
		queue 	= flags.queue,
		sort 	= flags.sort,
		page 	= flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
		builds	= req.builds || "*",
		search 	= flags.search,		
		pivot	= flags.pivot,
		browse	= flags.browse,
		tree	= flags.tree,
		builds 	= flags.build || "*",
		
		// record locking parameters

		lockID = `${table}.${log.RecID}`, 			// id of record lock
		lock = req.lock = LOCKS[lockID], 			// record lock
		locking = flags.lock ? true : false;
		
		// CRUDE response interface
		function sqlSelect(res) {

			Trace( sql.query(

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
					+ ( page 	? Page( page ) : "" ),

				[table, Guard(query,true), page], function (err, recs) {
							
					res(err || recs);

			}));

		}
		
		function sqlDelete(res) {
				
			if (false) 		// queue support (enable if useful)
				sql.query("SELECT Name FROM ?? WHERE ?",[table,query])
				.on("result", function (rec) {

					sql.query("DELETE FROM queues WHERE least(?,1) AND Departed IS NULL", {
						Client: "system",
						Class: queue,
						Job: rec.Name
					});

				});

			sql.query( 						// attempt journal
				"INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", 
				journal)
				
			.on("end", function () {
				
				Trace( sql.query( 		// sqlDelete
					"DELETE FROM ?? WHERE least(?,1)" ,
				
					[table, query, Guard(query,false)], function (err,info) {
						
						res(err || info);					
				}));
				
			});
			
			//if (journal) 
			//else
			//	sql.query( 			// sqlDelete
			//		cmd, [table, query, Guard(query,false)], function (err,info) {
			//			res(err || info);
			//	}); 
		}
			
		function sqlUpdate(res) {

			sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)  // attempt journal
			.on("end", function () {
				
				Trace( sql.query(
				
					Guard(body,false)
					? "UPDATE ?? SET ? WHERE least(?,1)"
					: "#UPDATE IGNORED", 
					
					[table,body,Guard(query,false)], function (err,info) { 	// sqlUpdate
					
					res( err || info );
					
					if (queue && body[queue])   						// queue support
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
								State:req.body[queue],
								Arrived:new Date(),
								Notes:rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
							});
						});
				
					//sql.query("INSERT INTO ?? SELECT *,? as j_ID FROM ?? WHERE ?", journal)
					//.on("end", function () {
					//	sql.query( 								// sqlInsert
					//		cmd, [table,body,query], function (err,info) {
					//			res(err || info);
					//	});
					//});
				}));

			});
			
		}
			
		function sqlInsert(res) {

			Trace( sql.query(  	// sqlInsert
			
				Guard(body,false)
					? "INSERT INTO ?? SET ?" 
					: "INSERT INTO ?? () VALUES ()", 
					
				[table,body], function (err,info) {
				
				res(err || info);
				
				if (false && queue)  // queue support (can enable but empty job name results)
					sql.query("SELECT Name FROM ?? WHERE ?",[req.table,{ID:info.insertId}])
					.on("result", function (rec) {
						sql.query("INSERT INTO queues SET ?", {
							Client	: "system",
							Class	: queue,
							Job	: rec.Name,
							State	: body[queue],
							Arrived	: new Date(),
							Notes	: rec.Name.tag("a",{href:"/intake.view?name="+rec.Name})
						});	
					});
			}));	

		}
		
		function sqlExecute(res) {
			res( new Error("Execute Reserved") );
		}
		
	Trace(`${locking?"LOCK":""} ${action} FROM ${table} FOR ${client} ON ${CLUSTER.isMaster ? "MASTER" : "CORE"+CLUSTER.worker.id}`);

	if (locking) 				// Execute query in a locked transaction thread
		switch (action) {
			case "select":

				sqlSelect( function (recs) {

					if (recs.constructor == Error) 
						res( recs+"" );
					else
					if (rec = recs[0]) {
						var lockID = `${table}.${rec.ID}`,		// record lock name	
							lock = req.lock = LOCKS[lockID]; 	// record lock

						if (lock) {
							if (false && SQL.emit)
								SQL.emit( "update",  {
									table: lock.Table,
									body: {_Locked: true},
									ID: lock.RECID,
									from: lock.Client,
									flag: flags.client 
								});

							if (lock.Client == client) { 		// client owns this transaction

								lock.sql.query("COMMIT");
								delete LOCKS[lockID];
								req.lock = null;
								
								res( rec );
							}
							else {					// client does not own this transaction
								lock.Hits++;
								res( "locked by "+lock.Client );
							}
						}
						else 
							SQL.thread( function (txsql) {  	// reserve this thread for as long as it lives

								var lock = req.lock = LOCKS[lockID] = { 	// create a lock
									Table: table,
									RECID: rec.ID,
									Hits: 0,
									sql	: txsql,
									Client: client
								};

								if (false && SQL.emit)     // may prove useful to enable
									SQL.emit( "update",  {
										table: lock.Table,
										body: {_Locked: false},
										ID: lock.RECID,
										from: lock.Client,
										flag: flags.client 
									});

								txsql.query("START TRANSACTION", function (err) {  // queue this transaction

									res( rec );
									
								});
								
								// dont release txsql - timer being used
								// to t/o if the client locks the record too long
							});
					}
					else
						res( "null" );
				});
						
				break;
			
			case "delete":
				
				sqlDelete( res );
				break;
										
			case "insert":

				if (lock) 
					if (lock.Client == client) {
						sqlInsert( res );		

						lock.sql.query("COMMIT", function (err) {
							req.lock = null;
							delete LOCKS[lockID];
						});
					}
					else {
						lock.Hits++;
						res( "locked by "+lock.Client );
					}
				else 
					sqlInsert( res );

				break;
				
			case "update":

console.log(query);
console.log(body);

				if (lock) 
					if (lock.Client == client) {
						sqlUpdate(res);		

						lock.sql.query("COMMIT", function (err) {
							req.lock = null;
							delete LOCKS[lockID];
						});
					}
					else {
						lock.Hits++;
						res( "locked by "+lock.Client );
					}
				else 
					sqlUpdate(res);

				break;
				
			case "execute":
				
				res( "cant execute" );
				break;
				
			default:
			
				res( "invalid request" );
				break;
		}
	
	else 						// Execute non-locking query
		switch (action) {
			case "select": sqlSelect(res); break;
			case "update": sqlUpdate(res); break;
			case "delete": sqlDelete(res); break;
			case "insert": sqlInsert(res); break;
			case "execute": sqlExecute(res); break;
		}
	
	if (false && SQL.emit) 		// Notify clients of change.  
		SQL.emit( req.action, {
			table: req.table, 
			body: body, 
			ID: log.RecID, 
			from: req.client,   // Send originating client's ID so they cant ignore its own changes.
			flag: flags.client
		});

}
*/

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
		
	//$$$$
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

/*
function sqlEach(query, args, cb) {
	
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
	return " ORDER BY " + sorts.joinify( function (sort) {  //$$$$
		
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

	Each( query, function (n, arg) {
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

	Each( joins, function (join,on) {
		rtn.push( "LEFT JOIN "+join+" ON "+on );
	});
	
	return rtn.join(" ");
}

function Pivot(pivot, query, flags) {
	var	NodeID = query.NodeID || "root",
		nodes = (NodeID == "root") ? [] : NodeID.split(LIST);
	
	var group = flags.group = nodes.length ? "" : pivot.escape();

	if (group) {  		// at the root
		var rtn = 
			  ", " + (group.enclose("concat")+" as char").enclose("cast") + " as NodeID"
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

function Guard(query, def) {
	for (var n in query) return query;
	return def;
}
*/

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
				delete APP.queues[job.qos].batch[job.ID];
				job.qos = req.qos;
				APP.queues[qos].batch[job.ID] = job;
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

			delete APP.queues[job.qos].batch[job.ID];
			
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
										APP.thread( function (sql) {
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

//$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
/*
	Encapsulate an underlying (default MySQL) database table into a JS dataset variable:
	
		var ds = SQL.DSVAR(sql, { ATTRIBUTE: VALUE, ... })
	
	where ATTRIBUTEs are:
	
		table: 	"DB.TABLE" || "TABLE"
		where: 	[ FIELD, VALUE ] || [ FIELD, MIN, MAX ] || {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} || "CLAUSE"
		having: [ FIELD, VALUE ] || [ FIELD, MIN, MAX ] || {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} || "CLAUSE"
		order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] || "FIELD, ..."
		group: 	[ FIELD, ...] || "FIELD, ..."
		limit: 	[ START, COUNT ] || {start:START, count:COUNT} || "START,COUNT"
		build:	[ FIELD, ... ] || "FIELD, ... "
		res: 	function (ds) {...}
	
	Null attributes are ignored.  CRUD operations to underlying records are performed using:
	
		ds.rec = { FIELD:VALUE, ... }		// update matched record(s) 
		ds.rec = [ {...}, {...}, ... ]		// insert record(s)
		ds.rec = null 						// delete matched records matched 
		ds.rec = function F(data) {...}		// select F=each/all matched record(s)
		
	and will callback its non-null response .res method when completed.  The select operation
	returns the specified .build fields for F=each/all .where matched records, in the requested
	.order ordering and .group grouping, as filtered by .having and .limit attributes.

	CRUDE = "select" || "delete" || "update" || "insert" || "execute" operations can 
	also be performed using:
	
		ds.res = callback function
		ds.data = dataset
		ds.rec = CRUDE
		
	or in record-locked mode using:
	
		ds.rec = "lock." + CRUDE
	
	Additional attributes:
		
		attr	default	true/set to
		-----------------------------------------------------------------------------------
		unsafe 	true	execute potentially unsafe queries
		trace	false	display formed queries
		journal	false	enable table journalling
		ag		null	aggregate where/having = least(?,1), greatest(?,0), sum(?), ...
		nlp		null	search pattern using natural language parse
		bin		null	search pattern in binary mode
		qex		null	search pattern with query expansion
		
	A large context of datasets can be esatlished on the same sql connector with:
	
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
		var ds = new SQL.DSVAR(sql,{table:"test.x",trace:1,build:{NodeID:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});
		
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

SQL.DSVAR = function(sql,opts,defs) {
	
	this.sql = sql;
	this.err = null;
	this.query = "";
	this.opts = null;
	this.unsafe = true;
	
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
		
	for (var n in opts) this[n] = opts[n];
}

SQL.DSVAR.prototype = {
	
	x: function xquery(opt,key,buf) {
		
		function nodeify(list) {
			return "`"+list.split(",").join("`,',',`")+"`";
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

					if (me.browse) { 
						var	nodes = opt.NodeID ? opt.NodeID.split(",") : [];
						
						me.group = me.browse[nodes.length];

						if (me.group) { 
							var pivots = nodeify(browse.slice(0,nodes.length+1));
							
							me.query += ` ${key} *`;
							me.query += `,cast(concat(${pivots}) AS CHAR) AS NodeID`;
							me.query += ",count(ID) AS NodeCount";
							me.query += ",false AS leaf, true AS expandable, false AS expanded";
								
							nodes.each( function (n,node) {
								me.where[ browse[n] ] = node;
							});
						}
						else  {
							var pivots = nodeify(browse.concat(["ID"]));
							
							me.query += ` ${key} *`;
							me.query += `,cast(concat(${pivots}) AS CHAR) AS NodeID`;
							me.query += ",1 as NodeCount";
							me.query += ",true AS leaf, false AS expandable, true AS expanded";
								
							nodes.each( function (n,node) {
								me.where[(n >= browse.length) ? "ID" : browse[n]] = node;
							});
						}
					}
					
					else
					if (me.group)
						if (opt.NodeID) {
							var	nodes = (opt.NodeID == "root") ? [] : opt.NodeID.split(",");									
							var pivots = nodes.length ? "" : nodeify(me.group);

							if (pivots) {  		// at the root
								me.query += ` ${key} ${me.group}`;
								me.query += `,cast(concat(${pivots}) AS CHAR) AS NodeID`;
								me.query += ",count(ID) AS NodeCount"
								me.query += ",false AS leaf,true AS expandable,false AS expanded";
							}
							else {  					// requesting all nodes under a specific node
								me.query += ` ${key} `;
								me.query += "ID AS NodeID";
								me.query += ",1 AS NodeCount";
								me.query += ",true AS leaf,true AS expandable,false AS expanded";
							}
						}
						else {
							me.query += ` ${key} ??`;
							me.opts.push( (opt.vars||"ID").split(",") );
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
				
					switch (opt.constructor) {
						case Array:
						
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
							
							break;
							
						case String:
						
							if (opt) {
								me.safe = false;
								me.query += ` ${key} ${opt}`;
							}
							break;

						case Object:
							var rels = []; 
							for (var n in opt) {
								if (opt[n] == null) {
									me.safe = false;
									rels.push(n);
									delete opt[n];
								}
								else
								if ( (args = (opt[n]+"").split(",")).length>1 ) {
									me.safe = false;
									rels.push(sql.escapeId(n) + " BETWEEN " + args[0] + " AND " + args[1] );
									delete opt[n];
								}
							}
							
							for (var n in opt) {
								rels.push(ag);
								me.opts.push(opt);
								break;
							}
									
							rels = rels.join(" AND ");
							if (rels)
								me.query += ` ${key} ${rels}`;
								
							break;
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
			ID = me.where ? me.where.ID : 0,
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
		
		me.opts = []; me.query = ""; me.safe = true;
			
		me.x(table, "UPDATE");
		me.x(req, "SET");
		me.x(me.where, "WHERE "+(me.ag||""));
		me.x(me.order, "ORDER BY");
		
		if (me.safe || me.unsafe)
			journal( function () {
				
				sql.query(me.query, me.opts, function (err,info) {

					if (res) res( err || info );

				});

				if (client && SQL.emit) 		// Notify clients of change.  
					SQL.emit( "update", {
						table: me.table, 
						body: req, 
						ID: ID, 
						from: client
						//flag: flags.client
					});

				if (me.trace) Trace((me.safe?"":"UNSAFE")+me.query);

			});
		
		else
		if (res)
			res( new Error("unsafe query blocked") );
			
	},
	
	select: function (req,res) {

		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where ? me.where.ID : 0,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true;
			
		me.x(me.build || "*", "SELECT SQL_CALC_FOUND_ROWS");
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

		if (me.safe || me.unsafe)
			switch (req.name) {
				case "each":
					sql.query(me.query, me.opts)
					.on("error", function (err) {
						req(err);
					})						
					.on("result", function (rec) {										
						req(rec);
					}); 
					break;
				
				case "all":
				default:
					sql.query(me.query, me.opts, function (err,recs) {	
						req( err || recs );
					});
			}
		
		else
		if (res)
			res( new Error("unsafe query blocked") );
		
		if (me.trace) Trace((me.safe?"":"UNSAFE")+me.query);
					
	},
	
	delete: function (req,res) {
		
		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where ? me.where.ID : 0,
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
		
		me.opts = []; me.query = ""; me.safe = true;
		
		me.x(table, "DELETE FROM");
		me.x(me.where, "WHERE "+(me.ag||""));
		
		if (me.safe || me.unsafe)
			journal( function () {
			
				me.sql.query(me.query, me.opts, function (err,info) {

					if (me.res) me.res(err || info);
					
				});
					
				if (me.client && SQL.emit) 		// Notify clients of change.  
					SQL.emit( "delete", {
						table: me.table, 
						ID: ID, 
						from: me.client
						//flag: flags.client
					});
				
				if (me.trace) Trace((me.safe?"":"UNSAFE")+me.query);
				
			});
	
		else
		if (res)
			res( new Error("unsafe query blocked") );		
	},
	
	insert: function (req,res) {
		
		var	me = this,
			table = SQL.DBTX[me.table] || me.table,
			ID = me.where ? me.where.ID : 0,
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
		
		me.opts = []; me.query = ""; me.safe = true;
		
		req.each(function (n,rec) {
			if (rec)
				sql.query(
					me.query = " INSERT INTO ?? SET ?", [table,rec], function (err,info) {

					if (!n && res) {
						res( err || info );

						if (client && SQL.emit) 		// Notify clients of change.  
							SQL.emit( "insert", {
								table: me.table, 
								body: rec, 
								ID: info.insertId, 
								from: client
								//flag: flags.client
							});
					}			

				});

			if (me.trace) Trace((me.safe?"":"UNSAFE")+me.query);
		});

	},

	get rec() { 
	},
	
	unlock: function (ID, cb, lockcb) {
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

	set job(req) {
	},
	
	set rec(req) {
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
						case "execute": me.rec = new Error("execute undefined"); break;
						
						default:
							me.rec = new Error("invalid request");
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
	for (var n in ctx) context[n] = new SQL.DSVAR(sql,ctx[n],{table:"test."+n});
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
			group: 	flags.pivot,
			limit: 	flags.limit ? [ Math.max(0,parseInt( flags.start || "0" )), Math.max(0, parseInt( flags.limit || "0" )) ] : null,
			build: 	flags.build,
			data:	req.body,
			client: req.client,
			unsafe:	false	
		}}, function (ctx) {
			
		ctx.ds.rec = (flags.lock ? "lock." : "") + req.action;
		
	});
}


// UNCLASSIFIED
