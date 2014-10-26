var influx = require('influx');
var _ = require('underscore');

var name = "influx-store";

function jsonMapper(col, pts){
	var obj = {};
	if(col.length === pts.length){
		for(var i=0; i < col.length; i++){
			obj[col[i]] = pts[i];
		}
	}
	return obj;
}

function generateId(){
	var str = '';
	var i;	
	for (i = 0; i < 12; i++) {
	  var number = Math.floor(Math.random() * 10) % 10;
	  str += number;
	}
  return parseInt(str);
}

function getIdRange(id){
	var lower = id * 1000;
	var upper = ((id + 1) * 1000) - 1;

	return 	{
						"lower": lower,
						"upper": upper
					}; 
}

function metaquery(qent,q) {
  var mq = {}

  

    if( q.sort$ ) {
      for( var sf in q.sort$ ) break;
      var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending'
      mq.sort = [[sf,sd]]
    }

    if( q.limit$ ) {
      mq.limit = q.limit$
    }

    if( q.skip$ ) {
      mq.skip = q.skip$
    }

    if( q.fields$ ) {
      mq.fields = q.fields$
    }

  return mq
}

function fixquery(qent,q) {
  var qq = {};

  if( !q.native$ ) {
    for( var qp in q ) {
      if( !qp.match(/\$$/) ) {
        qq[qp] = q[qp]
      }
    }
  }
  
  return qq
}



module.exports = function(opts) {
	var seneca = this, 
		specifications = null
		dbInst = null;

	// Configure a influxdb instance
	var configure = function (spec,cb) {
		specifications = spec;
		dbInst = influx(spec);
		console.log("dbInst", dbInst);
		return cb(null, dbInst);
	}
	

	var store = {
	  name:name,

    close: function(args,cb) {

    },

    //todo check if dbInst is null
    save: function(args,cb) {
    
    	var ent = args.ent
      var update = !!ent.id;
      var canon = ent.canon$({object:true});
      var seriesName = (canon.base?canon.base+'_':'')+canon.name;
      var id = 0;
      var timeRange = null;
      var range;
      
      if(dbInst){
      	var entp = {};

        var fields = ent.fields$()
       
        fields.forEach( function(field) {
          
          /*Hack - influx doesnt support nested objects*/
          if(typeof ent[field] == "object"){
          	ent[field] = JSON.stringify(ent[field]);
          }

          entp[field] = ent[field]
        })

        if(update){
        	dbInst.writePoint(seriesName, entp, function(err){
        		if(err){
        			return cb(err);
        		} else {
        			range = getIdRange(entp.id);
        			
        			//Fetch previously written record from db
        			var query = 'select * from ' + seriesName + ' where time > ' + range.lower + 'u and time < ' + range.upper + 'u';
			      	
			      	dbInst.query(query, function(err, entity){
			      		
			      		
			      		if(err){
			      			seneca.log.error('entity', err);
			      			return cb(err, null)
			      		} else {
			      			
			      			if(entity.length === 0){
			      				return cb(null, null);
			      			}
			      			
			      			mObj = jsonMapper(entity[0]['columns'], entity[0]['points'][0]);
			      			fent = ent.make$(mObj);
			      			
			      			return cb(null, fent);
			      		}
			      	});
        		}

        	});

        } else {
        	id = generateId();
          
          entp.time = id;
          entp.id = id;

        	range = getIdRange(id);

        	dbInst.writePoint(seriesName, entp, function(err){
        		if(err){
 
        			return cb(err);
        		} else {
        			
        			//after writing query db
        			var query = 'select * from ' + seriesName + 
        									' where time > ' + range.lower + 'u and time < ' + range.upper + 'u';

			      	dbInst.query(query, function(err, entity){
			      		
			      		
			      		if(err){
			      			seneca.log.error('entity', err);
			      			return cb(err, null);
			      		} else {
			      			
			      			if(entity.length === 0){
			      				return cb(null, null);
			      			}
			      			
			      			mObj = jsonMapper(entity[0]['columns'], entity[0]['points'][0]);
			      			fent = ent.make$(mObj);

			      			/*hack - influxdb doesnt support nested*/
						    	for (prop in fent) {
									    if (!fent.hasOwnProperty(prop)) {
									        continue;
									    }
									    if(fent[prop] == "{" || fent[prop] == "["){
									    	fent[prop] = JSON.parse(fent[prop]);
									    }
									}
			      			return cb(null, fent);
			      		}
			      	});
        		}
        	});
        }
      } else {
      	seneca.log.error('db', 'Cannot connect to db');
      }
    },

    load: function(args,cb) {
    	var qent = args.qent;
      var q    = args.q;
      var ent = args.ent
      var mq = metaquery(qent,q);
      var mObj;
      var fent = null;
      var canon = ent.canon$({object:true});
      var seriesName = (canon.base?canon.base+'_':'')+canon.name;
      var range = getIdRange(q);


      if(dbInst){
      	var query = 'select * from ' + seriesName + 
      							' where time > ' + range.lower + 'u and time < ' + range.upper + 'u';
      	
      	dbInst.query(query, function(err, entity){
      		if(err){
      			 
      			 seneca.log.error('entity',err,{store:name})
      			 return cb(err, null)
      		} else {
      			if(entity.length === 0){
      				return cb(null, null);
      			}
      			mObj = jsonMapper(entity[0]['columns'], entity[0]['points'][0]);
      			fent = qent.make$(mObj);
   
      			return cb(null,fent);
      		}
      	});
      } else {
      	seneca.log.error('db', 'Cannot connect to db', {store:name});
      }
    },


    list: function(args,cb) {
      var qent = args.qent
      var q    = args.q
      var ent = args.ent
      var canon = ent.canon$({object:true});
      var seriesName = (canon.base?canon.base+'_':'')+canon.name;

      if(dbInst){
      	var mq = metaquery(qent,q)
        var qq = fixquery(qent,q)
      	
      	var whereConditions = "";

      	if(!_.isEmpty(args.q)){
      		
      		for (prop in qq) {
				    if (!qq.hasOwnProperty(prop)) {
				        continue;
				    }
				    if(typeof qq[prop] == "string"){
				    	qq[prop] = "'"+q[prop]+"'";
				    }
				    whereConditions = whereConditions + " " + prop + " = " + qq[prop] + " AND ";
					}

      		whereConditions = whereConditions.substring(0, whereConditions.length - 4);
      	}

				var whereClause = whereConditions != "" ? " WHERE " + whereConditions : "";
				
				var query = "SELECT * FROM " + seriesName + whereClause;
				
				dbInst.query(query, function(err, entities){
					var points;
					var list = [];
					if(err){
						return cb(err, null);
					} else {
						
						points = entities[0]['points'];
						columns = entities[0]['columns'];
						
						for(var i = 0; i < points.length; i++){
							var fent = null;
							var tempObj = jsonMapper(columns, points[i]);
							fent = qent.make$(tempObj)
							list.push(fent);
						}

						return cb(null, list);
					}
				});

      } else {
      	seneca.log.error('db', 'Cannot connect to db', {store:name});
      }
    },

    remove: function(args,cb) {
      var qent = args.qent
      var q    = args.q

      var all  = q.all$ // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$ // default true

      var ent = args.ent

      var canon = ent.canon$({object:true});
      var seriesName = (canon.base?canon.base+'_':'')+canon.name;


      if(dbInst){
      	
          var qq = fixquery(qent,q)

          if( all ) {
          	  var query = "DELETE FROM " + seriesName; 
            	dbInst.query(query, function(err, entity){
            		seneca.log.debug('remove/all',q,desc)
              	return cb(err)
            	}) 
          }
          else {
            var mq = metaquery(qent,q)
            //add limit 1
            if(qq.id){
          	  var range = getIdRange(qq.id);
	            var query = "DELETE FROM " + seriesName + 
	            						" WHERE time < " + range.upper + "u" +" AND time > " + range.lower + "u";

	            dbInst.query(query, function(err){
	 
	            	if(err){
	            		return cb(err, null);
	            	} 
	            });
            }
  
          }
        
      } else {
      	seneca.log.error('db', 'Cannot connect to db', {store:name});
      }

     
    },

    native: function(args,done) {
      
    }
  }

	var meta = seneca.store.init(seneca,opts,store)
	desc = meta.desc


	seneca.add({init:store.name,tag:meta.tag},function(args,done){
		configure(opts,function(err){
		  if( err ) return seneca.die('store',err,{store:store.name,desc:desc});
		  return done();
		})
	})


	return {name:store.name,tag:meta.tag}

};
