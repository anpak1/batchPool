/*
Batch module collects objects for store in memory and periodicaly flush it in dataBase.

Example usage:
	var workdb 	= mongoq(config.mongo.workdb, {safe:true, journal:false, j:false})
	var raw 	= workdb.collection('rawactions')

	var BatchPool = require('./batchPool.js');

	var batchPool = new BatchPool( config.batchPoolSize || 100, config.batchWaitingTime);
	batchPool.setSaver(raw);

	var obj = {
		key: 'value'
	}
	batchPool.push(obj);

	...

	// before exit do batchPool
	batchPool.flush(function(){
	  	process.exit(0);
	});
*/

'use strict';

var BatchPool = module.exports = function(maxSize, waitingTime){
	this._pool = [];
	this.maxSize = maxSize || 20;
	this.waitingTime = waitingTime || 6000;
	this._timerKey;
	this._flush;

	var self = this;
	this._flush = function(){
		self.flush();
		// delete self;
		self._timerKey = setTimeout(self._flush, self.waitingTime);
	}
	this._timerKey = setTimeout(this._flush, this.waitingTime);
};
// init database as saver object
BatchPool.prototype.setSaver = function(saver){
	this._saver = saver;
}
// push element to batch container
BatchPool.prototype.push = function(el){
	
	// clearTimeout(this._timerKey);
	// delete this._flush;

	this._pool.push(el);
	// console.log('pushed::', el, ' batch::', this._pool);
	this._pool.length >= this.maxSize && this.flush();
	
	// var self = this;
	// this._flush = function(){self.flush(); delete self;}
	// this._timerKey = setTimeout(this._flush, this.waitingTime);
}
// store batch to database
BatchPool.prototype.flush = function(cb){
	// console.log('flush start');
	var batch = this.clone();
	if (batch.length){
		this._saver.insert(batch, {continueOnError: true}).done(function(){
			// console.log('flush done', batch);
			delete batch;
			cb && cb();
		});	
	}else{
		delete batch;
		cb && cb();
	}
}
// clone current in-object container
BatchPool.prototype.clone = function(){
	var clone = this._pool;
	this._pool = [];
	return clone;
}
// return length of container elements
BatchPool.prototype.length = function(){
	return this._pool.length;
}