// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_cache.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <aio.h>
#include <signal.h>
////#include "include/rados/librados.hpp"
//using namespace librados;


#define dout_subsys ceph_subsys_rgw

//Mohammad
void handle_cache_read_cb(int signal, siginfo_t *info, void*uap);

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
	ldout(cct, 10) << "cache get: name=" << name << " : hit" << dendl;

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

		entry->chained_entries.push_back(make_pair<RGWChainedCache *, string>(chained_entry->cache, chained_entry->key));
	}

	return true;
}

void ObjectCache::put(string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
	RWLock::WLocker l(lock);

	if (!enabled) {
		return;
	}

	ldout(cct, 10) << "cache put: name=" << name << dendl;
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


void ObjectDataInfo::put(std::string oid){

	map<string, ChunkDataInfo>::iterator iter = chunk_list.find(oid);
	if (iter == chunk_list.end()) {
		ChunkDataInfo chunk_data_info;
		chunk_list.insert(pair<string, ChunkDataInfo>(oid, chunk_data_info));
		iter = chunk_list.find(oid);

		dout(0) << "Engage1: ObjectDataInfo::put chunk with oid =" << oid << "added to obj_key=" << obj_key << dendl;
	}

	ChunkDataInfo &entry = iter->second;
	entry.oid = oid;
	entry.address = "/mnt/raid0/cache/" + oid;
	entry.set_ctx(cct);
}

/*engage1*/
DataCache::DataCache () 
	: io_type(ASYNC_IO), index(0), lock("DataCache"), cache_lock("DataCache::Mutex"), req_lock("DataCache::req"), eviction_lock("DataCache::EvictionMutex"), free_data_cache_size(0), outstanding_write_size (0)
{ 
	tmp_data = new char[0x400000];
	action.sa_sigaction = handle_cache_read_cb;
	action.sa_flags = SA_SIGINFO;
	sigemptyset(&action.sa_mask);
	sigaction(SIG_ENG, &action, NULL);
}

int DataCache::write(bufferlist& bl ,unsigned int len, std::string oid) {

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
	return r;
}

void cache_aio_write_completion_cb(sigval_t sigval){

	cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
	c->priv_data->update_cache_map(c);
}


void DataCache::update_cache_map(cacheAioWriteRequest *c){

	ChunkDataInfo  chunk_info;

	::close(c->cb->aio_fildes);
	free((void *)c->cb->aio_buf);
	c->cb->aio_buf = NULL;

	cache_lock.Lock();
	outstanding_write_list.remove(c->oid);
	chunk_info.oid = c->oid; 
	chunk_info.set_ctx(cct);
	chunk_info.size = c->cb->aio_nbytes;
	cache_map.insert(pair<string, ChunkDataInfo>(c->oid, chunk_info));
	map<string, ChunkDataInfo>::iterator iter = cache_map.find(c->oid);
	iter = cache_map.find(c->oid);
	cache_lock.Unlock();	

	eviction_lock.Lock();	
	free_data_cache_size -= c->cb->aio_nbytes;
	outstanding_write_size -=  c->cb->aio_nbytes;
	eviction_lock.Unlock();	
}

int DataCache::create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid){

	struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest;	
	int r=0;
	std::string location = "/mnt/raid0/cache/" + oid;

	struct aiocb *cb =new struct aiocb;
	mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
	memset(cb, 0, sizeof(struct aiocb));
	cb->aio_fildes = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
	if ((r = cb->aio_fildes) < 0)
	{
		dout(0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
		goto done;
	}

	cb->aio_buf = malloc(len);
	if(!cb->aio_buf)
	{
		dout(0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
		goto close_file;
	}

	memcpy((void *)cb->aio_buf, bl.c_str(),len);
	cb->aio_nbytes = len;
	cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	cb->aio_sigevent.sigev_notify_function = cache_aio_write_completion_cb;
	cb->aio_sigevent.sigev_notify_attributes = NULL;
	cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;

	wr->cb = cb;
	wr->oid = oid;
	wr->priv_data = this;

	if((r= ::aio_write(cb)) != 0) {
		dout( 0) << "ERROR: aio_write "<< r << dendl;
		goto free_buf;
	}

	return 0;

free_buf:
	free((void*)cb->aio_buf);
close_file:
	::close(cb->aio_fildes);
done:
	return r;
}




void DataCache::handle_data(bufferlist& bl, unsigned int len, std::string oid){

	int r = 0;
	size_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

        cache_lock.Lock();
        map<string, ChunkDataInfo>::iterator iter = cache_map.find(oid);
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

bool DataCache::get(std::string oid){

	bool exist = false;
	cache_lock.Lock();
	map<string, ChunkDataInfo>::iterator iter = cache_map.find(oid);
	if (!(iter == cache_map.end())){
		// check inside cache whether file exists or not!!!! then make exist true;
		exist = true;
	}
	cache_lock.Unlock();
	return exist;

}

/*engage1*/
void DataCache::put(const std::string& obj_key , const std::string& oid){}

int DataCache::read(bufferlist *bl, unsigned int len, std::string oid){

	std::string location = "/mnt/raid0/cache/" + oid; /* replace tmp with the correct path from config file*/
	int cache_file = -1;
	int r = 0;

	cache_file = ::open(location.c_str(), O_RDONLY);
	if (cache_file < 0)
	{
		dout(0) << "ERROR: DataCache:: read ::open the file has return error " << r << dendl;
		return -1;
	}
	r = ::read(cache_file, tmp_data, len);
	if (r < 0){
		dout(0) << "ERROR: DataCache::read: read from file has return error " << r << dendl;
		goto END;
	}
	bl->append(tmp_data, len);

END:
	::close(cache_file);

	return r;
}

void handle_cache_read_cb(int signal, siginfo_t *info, void*uap) {

	librados::cacheAioRequest *c = (librados::cacheAioRequest *)info->si_value.sival_ptr;
	c->op_data->cache_get_completed_ios(c);
}

int DataCache::aio_read(librados::cacheAioRequest *cc) {

	int r = 0;	
	req_lock.Lock(); 
	if((r= ::aio_read(cc->paiocb)) != 0) {
		dout(0) << "ERROR: DataCache::aio_read ::aio_read"<< r << dendl; 
	}
	req_lock.Unlock();
	return r;
}

void DataCache::clear_timer() {
}

size_t DataCache::random_eviction(){

	cache_lock.Lock();

	int cache_size = cache_map.size();
	if (cache_size <= 0 ){
		cache_lock.Unlock();
		return -1;
	}
	srand (time(NULL));
	int rand_num = rand()%cache_size;
	map<string, ChunkDataInfo>::iterator iter = cache_map.begin();
	std::advance(iter,rand_num);
	std::string oid = iter->first;
	ChunkDataInfo *entry =  &iter->second;
	dout( 0) << "ERROR: random eviction:   "<< rand_num << ", entry size is 0x" << std::hex << entry->size << dendl;
	size_t len = entry->size;
	cache_map.erase(oid);
	cache_lock.Unlock();
	std::string location = "/mnt/raid0/cache/" + oid; /* replace tmp with the correct path from config file*/
	remove(location.c_str()); 
	return len;
}




