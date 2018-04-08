/**
@class FLEX
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/flex.git) 
	[geointapps](https://git.geointapps.org/acmesds/flex)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/flex.git)

FLEX provides job queueing, job agents, file uploading, dataset emulation, 
engine plugins, dataset schema editor, email, new feeder, and client quizzing 
support to [DEBE](/api.view) via CRUDE interfaces to the following virtual datasets:

	CRUDE keyedits edit plug keys
	CRUE uploads	get/upload file(s) into one-time file upload area
	CRUE stores	get/upload file(s) into monitored file store area
	RE baseline local repo history, commit changes, sync with remote repo
	RE email list / autoflush (send/get) SMTP email
	RE catalog	read/flatten the master catalog
	RE news			
	RE issues		
	RE detectors
	RE baseline
	RE tasks list/autoupdate tasks
	R likeus provide client addition credit to run jobs
	R history	allows moderators to approve requirement changes etc
	R plugins list avaliable plugins
	R activity
	R views
	R summary
	R users
	R config
	R datasets
	R admin
	R cliques
	R health
	R likeus
	R tips
	R history
	R AlgorithmService Hydra job agent
	R parms		 
	R roles			
	R lookups				
	R searches
	R swaps		 	
	E milestones 		
	E sessions	
	E catalog flatten catalog

Legacy emulated datasets:

	collects	 	
	events		 		
	json edit a json formatted string 
 	aspreqts	 	
	ispreqts		
	hawks
	engine run simulation engine 

## Using

Each configuration follow the 
[ENUM deep copy() conventions](https://github.com/acmesds/enum):

	var FLEX = require("flex").config({
		key: value, 						// set key
		"key.key": value, 					// indexed set
		"key.key.": value					// indexed append
	}, function (err) {
		console.log( err ? "something evil is lurking" : "look mom - Im running!");
	});

where its [key:value options](/shares/prm/debe/index.html) override the defaults.

## Installing

Clone from one of the repos into your PROJECT/flex, then:

	cd PROJECT/flex
	ln -s PROJECT/totem/test.js test.js 			# unit testing
	ln -s PROJECT/totem/maint.sh maint.sh 		# test startup and maint scripts

Dependencies:
* [ENUM basic enumerators](https://github.com/acmesds/enum)
* [ATOMIC cloud compute](https://github.com/acmesds/atomic) 
* openv.X and app.X datasets as required by virtual tables

## Contributing

See our [issues](/issues.view), [milestones](/milestones.view), [s/w requirements](/swreqts.view),
and [h/w requirements](/hwreqts.view).

## License

[MIT](LICENSE)

*/