// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_cache.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

//#include <curl/curl.h> /*engage1*/
//#include <curl/types.h> /*engage1*/
//#include <curl/easy.h> /*engage1*/

#define dout_subsys ceph_subsys_rgw

using namespace std;

int ObjectCache::get(string& name, ObjectCacheInfo& info, uint32_t mask, rgw_cache_entry_info *cache_info)
{
	RWLock::RLocker l(lock);

	if (!enabled) {
		return -ENOENT;
	}

	map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
	if (iter == cache_map.end()) {
		ldout(cct, 10) << "cache get: name=" << name << " : miss" << dendl;
		if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
		return -ENOENT;
	}

	ObjectCacheEntry *entry = &iter->second;

	if (lru_counter - entry->lru_promotion_ts > lru_window) {
		ldout(cct, 20) << "cache get: touching lru, lru_counter=" << lru_counter << " promotion_ts=" << entry->lru_promotion_ts << dendl;
		lock.unlock();
		lock.get_write(); /* promote lock to writer */

		/* need to redo this because entry might have dropped off the cache */
		iter = cache_map.find(name);
		if (iter == cache_map.end()) {
			ldout(cct, 10) << "lost race! cache get: name=" << name << " : miss" << dendl;
			if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
			return -ENOENT;
		}

		entry = &iter->second;
		/* check again, we might have lost a race here */
		if (lru_counter - entry->lru_promotion_ts > lru_window) {
			touch_lru(name, *entry, iter->second.lru_iter);
		}
	}

	ObjectCacheInfo& src = iter->second.info;
	if ((src.flags & mask) != mask) {
		ldout(cct, 10) << "cache get: name=" << name << " : type miss (requested=" << mask << ", cached=" << src.flags << ")" << dendl;
		if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
		return -ENOENT;
	}
	ldout(cct, 10) << "cache get: name=" << name << " : hit (requested=" << mask << ", cached=" << src.flags << ")" << dendl;

	info = src;
	if (cache_info) {
		cache_info->cache_locator = name;
		cache_info->gen = entry->gen;
	}
	if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

	return 0;
}

bool ObjectCache::chain_cache_entry(list<rgw_cache_entry_info *>& cache_info_entries, RGWChainedCache::Entry *chained_entry)
{
	RWLock::WLocker l(lock);

	if (!enabled) {
		return false;
	}

	list<rgw_cache_entry_info *>::iterator citer;

	list<ObjectCacheEntry *> cache_entry_list;

	/* first verify that all entries are still valid */
	for (citer = cache_info_entries.begin(); citer != cache_info_entries.end(); ++citer) {
		rgw_cache_entry_info *cache_info = *citer;

		ldout(cct, 10) << "chain_cache_entry: cache_locator=" << cache_info->cache_locator << dendl;
		map<string, ObjectCacheEntry>::iterator iter = cache_map.find(cache_info->cache_locator);
		if (iter == cache_map.end()) {
			ldout(cct, 20) << "chain_cache_entry: couldn't find cachce locator" << dendl;
			return false;
		}

		ObjectCacheEntry *entry = &iter->second;

		if (entry->gen != cache_info->gen) {
			ldout(cct, 20) << "chain_cache_entry: entry.gen (" << entry->gen << ") != cache_info.gen (" << cache_info->gen << ")" << dendl;
			return false;
		}

		cache_entry_list.push_back(entry);
	}


	chained_entry->cache->chain_cb(chained_entry->key, chained_entry->data);

	list<ObjectCacheEntry *>::iterator liter;

	for (liter = cache_entry_list.begin(); liter != cache_entry_list.end(); ++liter) {
		ObjectCacheEntry *entry = *liter;

		entry->chained_entries.push_back(make_pair(chained_entry->cache, chained_entry->key));
	}

	return true;
}

void ObjectCache::put(string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
	RWLock::WLocker l(lock);

	if (!enabled) {
		return;
	}

	ldout(cct, 10) << "cache put: name=" << name << " info.flags=" << info.flags << dendl;
	map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
	if (iter == cache_map.end()) {
		ObjectCacheEntry entry;
		entry.lru_iter = lru.end();
		cache_map.insert(pair<string, ObjectCacheEntry>(name, entry));
		iter = cache_map.find(name);
	}
	ObjectCacheEntry& entry = iter->second;
	ObjectCacheInfo& target = entry.info;

	for (list<pair<RGWChainedCache *, string> >::iterator iiter = entry.chained_entries.begin();
			iiter != entry.chained_entries.end(); ++iiter) {
		RGWChainedCache *chained_cache = iiter->first;
		chained_cache->invalidate(iiter->second);
	}

	entry.chained_entries.clear();
	entry.gen++;

	touch_lru(name, entry, entry.lru_iter);

	target.status = info.status;

	if (info.status < 0) {
		target.flags = 0;
		target.xattrs.clear();
		target.data.clear();
		return;
	}

	if (cache_info) {
		cache_info->cache_locator = name;
		cache_info->gen = entry.gen;
	}

	target.flags |= info.flags;

	if (info.flags & CACHE_FLAG_META)
		target.meta = info.meta;
	else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
		target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

	if (info.flags & CACHE_FLAG_XATTRS) {
		target.xattrs = info.xattrs;
		map<string, bufferlist>::iterator iter;
		for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
			ldout(cct, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
		}
	} else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
		map<string, bufferlist>::iterator iter;
		for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
			ldout(cct, 10) << "removing xattr: name=" << iter->first << dendl;
			target.xattrs.erase(iter->first);
		}
		for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
			ldout(cct, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
			target.xattrs[iter->first] = iter->second;
		}
	}

	if (info.flags & CACHE_FLAG_DATA)
		target.data = info.data;

	if (info.flags & CACHE_FLAG_OBJV)
		target.version = info.version;
}

void ObjectCache::remove(string& name)
{
	RWLock::WLocker l(lock);

	if (!enabled) {
		return;
	}

	map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
	if (iter == cache_map.end())
		return;

	ldout(cct, 10) << "removing " << name << " from cache" << dendl;
	ObjectCacheEntry& entry = iter->second;

	for (list<pair<RGWChainedCache *, string> >::iterator iiter = entry.chained_entries.begin();
			iiter != entry.chained_entries.end(); ++iiter) {
		RGWChainedCache *chained_cache = iiter->first;
		chained_cache->invalidate(iiter->second);
	}

	remove_lru(name, iter->second.lru_iter);
	cache_map.erase(iter);
}

void ObjectCache::touch_lru(string& name, ObjectCacheEntry& entry, std::list<string>::iterator& lru_iter)
{
	while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
		list<string>::iterator iter = lru.begin();
		if ((*iter).compare(name) == 0) {
			/*
			 * if the entry we're touching happens to be at the lru end, don't remove it,
			 * lru shrinking can wait for next time
			 */
			break;
		}
		map<string, ObjectCacheEntry>::iterator map_iter = cache_map.find(*iter);
		ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
		if (map_iter != cache_map.end())
			cache_map.erase(map_iter);
		lru.pop_front();
		lru_size--;
	}

	if (lru_iter == lru.end()) {
		lru.push_back(name);
		lru_size++;
		lru_iter--;
		ldout(cct, 10) << "adding " << name << " to cache LRU end" << dendl;
	} else {
		ldout(cct, 10) << "moving " << name << " to cache LRU end" << dendl;
		lru.erase(lru_iter);
		lru.push_back(name);
		lru_iter = lru.end();
		--lru_iter;
	}

	lru_counter++;
	entry.lru_promotion_ts = lru_counter;
}

void ObjectCache::remove_lru(string& name, std::list<string>::iterator& lru_iter)
{
	if (lru_iter == lru.end())
		return;

	lru.erase(lru_iter);
	lru_size--;
	lru_iter = lru.end();
}

void ObjectCache::set_enabled(bool status)
{
	RWLock::WLocker l(lock);

	enabled = status;

	if (!enabled) {
		do_invalidate_all();
	}
}

void ObjectCache::invalidate_all()
{
	RWLock::WLocker l(lock);

	do_invalidate_all();
}

void ObjectCache::do_invalidate_all()
{
	cache_map.clear();
	lru.clear();

	lru_size = 0;
	lru_counter = 0;
	lru_window = 0;

	for (list<RGWChainedCache *>::iterator iter = chained_cache.begin(); iter != chained_cache.end(); ++iter) {
		(*iter)->invalidate_all();
	}
}

void ObjectCache::chain_cache(RGWChainedCache *cache) {
	RWLock::WLocker l(lock);
	chained_cache.push_back(cache);
}

/*engage1*/
int cacheAioWriteRequest::create_io(bufferlist& bl, unsigned int len, string oid) {
	std::string location = "/mnt/raid0/cache/" + oid;
	int r = 0;

	cb = new struct aiocb;
	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
	memset(cb, 0, sizeof(struct aiocb));
	r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
	if (fd < 0)
	{
		ldout(cct, 0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
		goto done;
	}
	cb->aio_fildes = fd;

	data = malloc(len);
	if(!data)
	{
		ldout(cct, 0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
		goto close_file;
	}
	cb->aio_buf = data;
	memcpy((void *)data, bl.c_str(), len);
	cb->aio_nbytes = len;
	goto done;	
free_buf:
	cb->aio_buf = NULL;
	free(data);
	data = NULL;

close_file:
	::close(fd);
done:
	return r;
}

DataCache::DataCache ()
	: index(0), lock("DataCache"), cache_lock("DataCache::Mutex"), req_lock("DataCache::req"), eviction_lock("DataCache::EvictionMutex"), cct(NULL), io_type(ASYNC_IO), free_data_cache_size(0), outstanding_write_size (0)
{
	tp = new L2CacheThreadPool(32);
}

/*ugur to do make it work buggggg */
int DataCache::io_write(bufferlist& bl ,unsigned int len, std::string oid) {

	ChunkDataInfo*  chunk_info = new ChunkDataInfo;

	std::string location = "/mnt/raid0/cache/" + oid; /* replace tmp with the correct path from config file*/
	FILE *cache_file = 0;
	int r = 0;

	cache_file = fopen(location.c_str(),"w+");
	if (cache_file <= 0)
	{
		dout(0) << "ERROR: DataCache::open file has return error " << r << dendl;
		return -1;
	}

	r = fwrite(bl.c_str(), 1, len, cache_file);
	if (r < 0) {
		dout(0) << "ERROR: DataCache::write: write in file has return error " << r << dendl;
	}

	fclose(cache_file);

	/*update cahce_map entries for new chunk in cache*/
	cache_lock.Lock();
	chunk_info->oid = oid;
	chunk_info->set_ctx(cct);
	chunk_info->size = len;
	cache_map.insert(pair<string, ChunkDataInfo*>(oid, chunk_info));
	cache_lock.Unlock();

	/*update free size*/
	//eviction_lock.Lock();
	//free_data_cache_size -= r;
	//outstanding_write_size -=  len;
	//eviction_lock.Unlock();

	return r;
}

void _cache_aio_write_completion_cb(sigval_t sigval) {

	cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
	c->priv_data->cache_aio_write_completion_cb(c);
}


void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest *c){

	ChunkDataInfo  *chunk_info = NULL;

	dout(0) << "engage: cache_aio_write_completion_cb oid:" << c->oid <<dendl;

	/* free allocated memory, close file descriptor*/
	//::close(c->cb->aio_fildes);
	//free((void *)c->cb->aio_buf);	
	//	c->cb->aio_buf = NULL;

	/*update cahce_map entries for new chunk in cache*/
	cache_lock.Lock();
	outstanding_write_list.remove(c->oid);
	chunk_info = new ChunkDataInfo;
	chunk_info->oid = c->oid;
	chunk_info->set_ctx(cct);
	chunk_info->size = c->cb->aio_nbytes;
	cache_map.insert(pair<string, ChunkDataInfo*>(c->oid, chunk_info));
	cache_lock.Unlock();

	/*update free size*/
	eviction_lock.Lock();
	free_data_cache_size -= c->cb->aio_nbytes;
	outstanding_write_size -=  c->cb->aio_nbytes;
	eviction_lock.Unlock();
	c->release();
	//delete c;
}

int DataCache::create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid){

	struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest(cct);
	int r=0;
	if (wr->create_io(bl, len, oid) < 0) {
		dout(0) << "engage: Error create_aio_write_request" << dendl;
		goto done;
	}
	//	std::string location = "/mnt/raid0/cache/" + oid;

	//	struct aiocb *cb =new struct aiocb;
	//	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
	//	memset(cb, 0, sizeof(struct aiocb));
	//	cb->aio_fildes = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
	//	if ((r = cb->aio_fildes) < 0)
	//	{
	//		dout(0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
	//		goto done;
	//	}
	//
	//	cb->aio_buf = malloc(len);
	//	if(!cb->aio_buf)
	//	{
	//		dout(0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
	//		goto close_file;
	//	}
	//
	//	memcpy((void *)cb->aio_buf, bl.c_str(),len);
	//	cb->aio_nbytes = len;
	wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	wr->cb->aio_sigevent.sigev_notify_function = _cache_aio_write_completion_cb;
	wr->cb->aio_sigevent.sigev_notify_attributes = NULL;
	wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
	wr->oid = oid;
	wr->priv_data = this;

	if((r= ::aio_write(wr->cb)) != 0) {
		dout( 0) << "ERROR: aio_write "<< r << dendl;
		goto error;
	}
	return 0;

	//free_buf:
	//	free((void*)cb->aio_buf);
	//close_file:
	//	::close(cb->aio_fildes);
	//done:
error:
	wr->release();
done:
//	delete wr;
	return r;
}

void DataCache::put(bufferlist& bl, unsigned int len, std::string oid){

	int r = 0;
	size_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

	dout(0) << "Engage1: We are in DataCache::put() and oid is: " << oid <<dendl;
	cache_lock.Lock();
	map<string, ChunkDataInfo *>::iterator iter = cache_map.find(oid);
	if (iter != cache_map.end()) {
		cache_lock.Unlock();
		dout(0) << "Engage1: Warning: data already cached, no rewirte" << dendl;
		return;
	}
	std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),oid);
	if (it != outstanding_write_list.end()) {
		cache_lock.Unlock();
		dout(0) << "Engage1: Warning: data put in cache already issued, no rewrite" << dendl;
		return;
	}
	outstanding_write_list.push_back(oid);
	cache_lock.Unlock();

	eviction_lock.Lock();
	_free_data_cache_size = free_data_cache_size;
	_outstanding_write_size = outstanding_write_size;
	eviction_lock.Unlock();

	while (len >= (_free_data_cache_size - _outstanding_write_size + freed_size)){
		r =random_eviction();
		if(r < 0)
			return;
		freed_size += r;
	}
	//      io_write(bl, len, oid);
	r = create_aio_write_request(bl, len, oid);
	if (r < 0) {
		cache_lock.Lock();
		outstanding_write_list.remove(oid);
		cache_lock.Unlock();
		dout(0) << "Engage1: create_aio_wirte_request fail, r=" << r << dendl;
		return;
	}

	eviction_lock.Lock();
	free_data_cache_size += freed_size;
	outstanding_write_size += len;
	eviction_lock.Unlock();
}

bool DataCache::get(string oid) { 

	bool exist = false;
	string location = "/mnt/raid0/cache/" + oid; 
	cache_lock.Lock();
	map<string, ChunkDataInfo*>::iterator iter = cache_map.find(oid);
	if (!(iter == cache_map.end())){
		// check inside cache whether file exists or not!!!! then make exist true;
		if(access(location.c_str(), F_OK ) != -1 ) { // file exists
			exist = true;
		} else {	
			cache_map.erase(oid);
			exist = false;
		}
	}
	cache_lock.Unlock();
	return exist;
}

size_t DataCache::random_eviction(){

	int n_entries = 0;
	int random_index = 0;
	size_t freed_size = 0;
	ChunkDataInfo *del_entry;
	string del_oid, location;

	cache_lock.Lock();
	n_entries = cache_map.size();
	if (n_entries <= 0){
		cache_lock.Unlock();
		return -1;
	}
	srand (time(NULL));
	random_index = rand()%n_entries;
	map<string, ChunkDataInfo*>::iterator iter = cache_map.begin();
	std::advance(iter, random_index);
	del_oid = iter->first;
	del_entry =  iter->second;
	dout( 0) << "INFO::random_eviction index:"<< random_index << ", free size:0x" << std::hex << del_entry->size << dendl;
	freed_size = del_entry->size;
	free(del_entry);
	del_entry = NULL;
	cache_map.erase(del_oid); // oid
	cache_lock.Unlock();

	location = "/mnt/raid0/cache/" + del_oid; /*replace tmp with the correct path from config file*/
	remove(location.c_str());
	return freed_size;
}

void DataCache::remote_io(struct librados::L2CacheRequest *l2request ) {
	dout( 0) << "Engage1: Add task to remote IO" << dendl;
	tp->addTask(new HttpL2Request(l2request, cct));
}


vector<string> split(const string &s, char * delim) {
	stringstream ss(s);
	string item;
	vector<string> tokens;
	while (getline(ss, item, (*delim))) {
		tokens.push_back(item);
	}
	return tokens;
}

string DataCache::hash_uri(string oid) {
	std::string::size_type sz;   // alias of size_t
	vector<string> sv = split(oid, "_");
	int hash = std::stoi(sv[sv.size()-1], &sz);
	int mod= 4;
	switch (hash%mod)
	{
		case 0:
			return "192.168.67.7:80";
		case 1:
			return "192.168.67.8:80";
		case 2:
			return "192.168.67.9:80";
		case 3:
			return "192.168.67.10:80";
	};

	return HASH_LOCAL;
}
