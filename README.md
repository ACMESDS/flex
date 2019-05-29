/**
@class FLEX
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/flex) 
	[geointapps](https://git.geointapps.org/acmesds/flex)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/flex)

# FLEX

FLEX provides plugins and virtual datasets to support: job queueing, job agents, file uploading, dataset emulation, 
engine plugins, dataset schema editor, email, new feeder, and client quizzing.

## CRUDE virtual datasets

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

## Usage

Access and configure FLEX like this:

	var FLEX = require("flex").config({
		key: value, 						// set key
		"key.key": value, 					// indexed set
		"key.key.": value					// indexed append
	}, function (err) {
		console.log( err ? "something evil is lurking" : "look mom - Im running!");
	});

where [its configuration keys](https://totem.west.ile.nga.ic.gov/shares/prm/flex/index.html) follow 
the [ENUM deep copy conventions](https://github.com/acmesds/enum).

## Installation

Clone [FLEX plugin interface](https://github.com/acmesds/flex) into your PROJECT/flex folder.  
Clone [ENUM basic enumerators](https://github.com/acmesds/enum) into your PROJECT/enum folder.  
Clone [ATOMIC cloud compute](https://github.com/acmesds/atomic) into your PROJECT/atomic folder.  

### Required MySQL Databases

* openv.X and app.X datasets as required by virtual tables

### Manage 

	npm run [ edit || start ]			# Configure environment
	npm test [ ? || X1 || X2 ... ]			# unit test
	npm run [ prmprep || prmload ]		# Revice PRM
	
## Contributing

To contribute to this module, see our [issues](https://totem.west.ile.nga.ic.gov/issues.view)
and [milestones](https://totem.west.ile.nga.ic.gov/milestones.view).

## License

[MIT](LICENSE)

*/