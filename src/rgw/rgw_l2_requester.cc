#include "rgw_l2_requester.h"
#include "rgw_rados.h"
#include "rgw_op.h"
#include "unistd.h"

#define dout_subsys ceph_subsys_rgw
//#define dout_subsys ceph_subsys_rgw
 
static size_t _l2_response_cb(void *ptr, size_t size, size_t nmemb, void* param) {

	librados::L2CacheRequest *req = (librados::L2CacheRequest *)param;
	req->pbl->append((char *)ptr, size*nmemb);
	return size*nmemb;
}
/*
int HttpL2Request::submit_bhttp_request(){ // blocking
       
	int r = 0; 
	CURLcode res;
	std::string& uri = *(new std::string);
        std::string& auth_token = *(new std::string);
        std::string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->len - 1);//start_str.str() + "-"+ end_str.str();
        int max_retries = 10;
	int n_retry = 0;
	
	get_obj_data *d = (get_obj_data *)req->op_data;
	((RGWGetObj_CB *)(d->client_cb))->get_req_info(req->dest, uri, auth_token);// Right now we have uri and auth_toke
	ldout(cct, 20) << "Engage1: uri:" << uri << dendl;	
retry:  if(curl_handle) {
                n_retry++;
		struct curl_slist *chunk = NULL;
                chunk = curl_slist_append(chunk, auth_token.c_str());
                curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
                res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
                curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
                curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
		//curl_easy_setopt(curl_handle, CURLOPT_BUFFERSIZE, req->len);
		//curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
                curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _l2_response_cb);
                curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
		//curl_slist_free_all(chunk); 
		curl_easy_reset(curl_handle);
	        if ((res = curl_easy_perform(curl_handle)) != CURLE_OK) { //run the curl command
			// rety for 10 times
                	ldout(cct, 20) << "Engage1: curl_easy_perform() failed, err:" << curl_easy_strerror(res) << ", retry:" << n_retry << dendl;
		//	if (n_retry <= max_retries) goto retry;
		        r = -1;
		//	goto free;
		}
        }

free:
        return r;
}
*/

int HttpL2Request::submit_bhttp_request(){ // blocking

        CURLcode res;

        std::string& uri = *(new std::string);
        std::string& auth_token = *(new std::string);
        std::string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->len - 1);//start_str.str() + "-"+ end_str.str();

	//CURL *curl = curl_easy_init();
        get_obj_data *d = (get_obj_data *)req->op_data;
        ((RGWGetObj_CB *)(d->client_cb))->get_req_info(req->dest, uri, auth_token);// Right now we have uri and auth_toke

        if(curl_handle) {
                struct curl_slist *chunk = NULL;
                chunk = curl_slist_append(chunk, auth_token.c_str());
                curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
                res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
                curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
                curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
	//	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
                curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _l2_response_cb);
                curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
                res = curl_easy_perform(curl_handle); //run the curl command
          //      curl_easy_cleanup(curl);
		curl_easy_reset(curl_handle);
		curl_slist_free_all(chunk);
        }

        if(res != CURLE_OK)
        {
                dout(0) << "Engage1: curl_easy_perform() failed " << curl_easy_strerror(res) << " oid " << req->oid << " offset " << req->ofs + req->read_ofs  <<dendl;
        return -1;
	}
       return 0;
}

void HttpL2Request::run() { 

        get_obj_data *d = (get_obj_data *)req->op_data;

        int n_retries = 10;
        int r=0;
        for (int i=0; i<n_retries; i++ ){
                if(!(r = submit_bhttp_request())){
                        d->cache_aio_completion_cb(req);
                        break;
                }
                dout(0) << "engage1: submit_bhttp_request retry "<< i << ", oid"<<req->oid << dendl;
                //usleep(100);
        }


}



/*
void HttpL2Request::run() { 

	get_obj_data *d = (get_obj_data *)req->op_data;
//	((RGWGetObj_CB *)(d->client_cb))->submit_l2_request(req->dest, req->ofs, req->len, req->read_ofs, (void *)req, _l2_response_cb);// Calls libcurl (blocking) 
	int r= submit_bhttp_request();//) {
	if (r<0)
	{
		dout(0) << "engage1: submit_bhttp_request another time oid:" << req->oid << dendl; 
		if (submit_bhttp_request() < 0)
		{
	 		return ;
		}
	}


	//}
		d->cache_aio_completion_cb(req);
	//else { // send to ceph
//		// check how to do that
//	}

}
*/
