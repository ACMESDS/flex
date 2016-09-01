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
		
		joinify: function (hash, list, cb) {
			switch (hash.constructor) {
				case Array:
					if (cb) {
						var rtn = [];

						for (var n=0, N=hash.length; n<N; n++) 
							rtn.push( cb( n, hash[n]) );
							
						return rtn.join(list);
					}
					else
						return hash.join(list);
						
				case Object:
					var rtn = [];

					for (var n in hash) 
						if (hash.hasOwnProperty(n)) 
							rtn.push( cb ? cb(n,hash[n]) : n + "=" + hash[n] );

					return rtn.join(list);
					
				default:	

					return hash;
			}
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
		select: sqlCrude,
		delete: sqlCrude,
		update: sqlCrude,
		insert: sqlCrude,
		execute: sqlCrude,
	
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

					//if (flags.find && !err && (table=="CATALOG")) 
						//hawkCatalog(req,res);
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

// CRUDE interface

/*
function Select(req,res) {
	sqlCrude(req, res);
}
function Update(req,res) {	
	sqlCrude(req,res);
}
function Insert(req,res) {	
	sqlCrude(req,res);
}
function Delete(req,res) {	
	sqlCrude(req,res);
}
function Execute(req,res) {	
	sqlCrude(req,res);
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
		
	flatten( sql, rtns, 0, ENUM.listify(catalog), catalog, limits, cb, function (search) {

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
	return " ORDER BY " + ENUM.joinify( sorts, LIST, function (n,sort) {
		
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

/**
 * Job queue interface.
 * 
 * select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
 * execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
 * update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
 * delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
 * insert(job,cb): add job and route to callback cb(job) when executed.
 * */

SQL.queues = [ null, {
		timer: 0,
		depth: 0,
		batch: {},
		rate: 10e3
	}, {
		timer: 0,
		depth: 0,
		batch: {},
		rate: 8e3
	}, {
		timer: 0,
		depth: 0,
		batch: {},
		rate: 4e3
	}, {
		timer: 0,
		depth: 0,
		batch: {},
		rate: 2e3
	}, {
		timer: 0,
		depth: 0,
		batch: {},
		rate: 1e3
	}];
	
function selectJobs(req, cb) { 

	// route valid jobs matching sql-where clause to its assigned callback cb(req).
	var sql = this;
	
	sql.query(
		req.where
		? `SELECT *, datediff(now(),Arrived) AS Age,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client WHERE NOT Hold AND Departed IS NULL AND NOT profiles.Banned AND (${req.where}) ORDER BY QoS,Priority`
		: `SELECT *, datediff(now(),Arrived) AS Age,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client WHERE NOT Hold AND Departed IS NULL AND NOT profiles.Banned ORDER BY QoS,Priority`
	)
	.on("error", function (err) {
		console.log(err);
	})
	.on("result", function (rec) {
		try {
			if (cb) cb( APP.queues[rec.QoS].batch[rec.ID] );
		}
		catch (err) {
			Trace("LOST job "+[rec.ID,rec.QoS]);
		}
	});	
}

function updateJobs(req, exe) { 
	// adjust priority of jobs matching sql-where clause and route to callback cb(req) when updated.
	
	var sql = this;
	
	sql.selectJobs(req, function (job) {
		
		exe(job.req, function (ack) {

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
		
function deleteJobs(req, exe) { 
	
	var sql = this;
	sql.selectJobs(req, function (job) {
		
		exe(job.req, function (ack) {
			sql.query("UPDATE queues SET ? WHERE ?", [{
				Departed: new Date(),
				Notes:ack}, {ID:job.ID}]);

			delete APP.queues[job.qos].batch[job.ID];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

function insertJobs(job, exe) { 
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
			
		job.cb = cb;
		var queue = SQL.queues[job.qos];
		
		if (queue) { 				// regulated job
			queue.batch[job.ID] = job;  // add job to queue
			queue.depth++;
			
//console.log(queue);

			if (!queue.timer) 		// restart idle queue
				queue.timer = setInterval(function (queue) {
					
					var batch = queue.batch;

					var pop = {priority: -1}; 
					Each( batch, function (ID,job) {
						if (job.priority > pop.priority) pop = job;
					});

					delete batch[pop.ID];
					queue.depth--;
				
//console.log("pop depth="+queue.depth+" pop="+[pop.name,pop.qos]);

					if (pop.cmd)	// spawn pop and return its pid
						pop.pid = CP.exec(pop.cmd, {cwd: "./public/dets", env:process.env}, function (err,stdout,stderr) {
							Trace(err + stdout + stderr);
							
							if (pop.cb)
								APP.thread( function (sql) {
									pop.cb( err ? err + stdout + stderr : null );
								});
						});
					else  			// execute pop cb on new sql thread
					if (pop.cb) 
						SQL.thread( function (sql) {
							pop.req.sql = sql;  // give job this fresh sql connector								
							pop.cb(sql,pop);
						});
				
					if (!queue.depth) { 	// empty queue goes idle
						clearInterval(queue.timer);
						queue.timer = null;
					}

				}, queue.rate, queue);
		}
		else 						// unregulated job - stay on request sql thread
		if (cb) cb( job.req.sql, job );
	}

	var rec = {
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
		KeyID	: job.key || "",
		Notes	: "queued",
		QoS		: job.qos,
		Work 	: 1
	};

	var sql = this;
	
	sql.query("INSERT INTO queues SET ?", rec, function (err,info) {
		
		if (err) 
			Trace("DROPPED JOB: "+err);
			
		else {
			job.ID = info.insertId;
			regulate(job, function (sql,job) {
				
				exe(job.req, function (ack) {

					var Departed = new Date();

					sql.query("UPDATE queues SET ? WHERE ?", [{
						Departed: Departed,
						RunTime: (Departed - rec.Arrived)/3.6e6,
						Util: util(),
						Notes: ack
					}, {ID:job.ID}
					]);

					Trace("COMPLETED JOB: "+ack);
				}); 
			
			});
		}
	});
}
	
function executeJobs(req, exe) {
}

// UNCLASSIFIED
