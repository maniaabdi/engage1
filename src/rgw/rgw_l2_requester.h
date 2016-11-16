/*engage1*/
#ifndef _L2_REQUESTER_
#define _L2_REQUESTER_

//#include <stdio.h>
#include <queue>
//#include <unistd.h>
//#include <pthread.h>
//#include <malloc.h>
//#include <stdlib.h>
//#include <assert.h>
//#include <curl/curl.h> 
//#include <curl/easy.h> 
#include "rgw_rados.h"

// Reusable thread class
class L2Thread
{
	public:
		L2Thread() 
		{
			state = EState_None;
			handle = 0;
		}

		virtual ~L2Thread()
		{
			assert(state != EState_Started || joined);
			curl_easy_cleanup(curl);
		}

		void start()
		{
			assert(state == EState_None);
			/* Must initialize libcurl before any threads are started */ 
  			//curl_global_init(CURL_GLOBAL_ALL);
			curl= curl_easy_init();
			// in case of thread create error I usually FatalExit...
			if (pthread_create(&handle, NULL, threadProc, this))
				abort();
			state = EState_Started;
		}

		void join()
		{
			// A started thread must be joined exactly once!
			// This requirement could be eliminated with an alternative implementation but it isn't needed.
			assert(state == EState_Started);
			pthread_join(handle, NULL);
			state = EState_Joined;
		}

	protected:
		virtual void run() = 0;

	private:
		static void* threadProc(void* param)
		{
			L2Thread* thread = reinterpret_cast<L2Thread*>(param);
			thread->run();
			return NULL;
		}

	private:
		enum EState
		{
			EState_None,
			EState_Started,
			EState_Joined
		};

		EState state;
		bool joined;
		pthread_t handle;
protected:
		CURL *curl;
};


// Base task for Tasks
// run() should be overloaded and expensive calculations done there
// showTask() is for debugging and can be deleted if not used
class Task {
	public:
		Task() {}
		virtual ~Task() {}
		virtual void run()=0;
		virtual void set_handler(void*) = 0;
};


// Wrapper around std::queue with some mutex protection
class WorkQueue
{
	public:
		WorkQueue() {
			pthread_mutex_init(&qmtx,0);
			pthread_cond_init(&wcond, 0);
		}

		~WorkQueue() {
			pthread_mutex_destroy(&qmtx);
			pthread_cond_destroy(&wcond);
		}

		// Retrieves the next task from the queue
		Task *nextTask() {
			// The return value
			Task *nt = 0;

			// Lock the queue mutex
			pthread_mutex_lock(&qmtx);

			while (tasks.empty())
				pthread_cond_wait(&wcond, &qmtx);

			nt = tasks.front();
			tasks.pop();

			// Unlock the mutex and return
			pthread_mutex_unlock(&qmtx);
			return nt;
		}
		// Add a task
		void addTask(Task *nt) {
			// Lock the queue
			pthread_mutex_lock(&qmtx);
			// Add the task
			tasks.push(nt);
			// signal there's new work
			pthread_cond_signal(&wcond);
			// Unlock the mutex
			pthread_mutex_unlock(&qmtx);
		}

	private:
		std::queue<Task*> tasks;
		pthread_mutex_t qmtx;
		pthread_cond_t wcond;
};

// Thanks to the reusable thread class implementing threads is
// simple and free of pthread api usage.
class PoolWorkerThread : public L2Thread
{
	public:
		PoolWorkerThread(WorkQueue& _work_queue) : work_queue(_work_queue) {}
	protected:
		virtual void run()
		{
			while (Task* task = work_queue.nextTask()) {
				task->set_handler((void *)curl);
				task->run();
			}
		}
	private:
		WorkQueue& work_queue;
};

class L2CacheThreadPool {
	public:
		// Allocate a thread pool and set them to work trying to get tasks
		L2CacheThreadPool(int n) {
			//printf("Creating a thread pool with %d threads\n", n);
			curl_global_init(CURL_GLOBAL_ALL);	
			for (int i=0; i<n; ++i)
			{
				threads.push_back(new PoolWorkerThread(workQueue));
				threads.back()->start();
			}
		}

		// Wait for the threads to finish, then delete them
		~L2CacheThreadPool() {
			finish();
		}

		// Add a task
		void addTask(Task *nt) {
			workQueue.addTask(nt);
		}

		// Asking the threads to finish, waiting for the task
		// queue to be consumed and then returning.
		void finish() {
			for (size_t i=0,e=threads.size(); i<e; ++i)
				workQueue.addTask(NULL);
			for (size_t i=0,e=threads.size(); i<e; ++i)
			{
				threads[i]->join();
				delete threads[i];
			}
			threads.clear();
		}

	private:
		std::vector<PoolWorkerThread*> threads;
		WorkQueue workQueue;
};

// stdout is a shared resource, so protected it with a mutex
static pthread_mutex_t console_mutex = PTHREAD_MUTEX_INITIALIZER;

// Debugging function

// Task to compute fibonacci numbers
// It's more efficient to use an iterative algorithm, but
// the recursive algorithm takes longer and is more interesting
// than sleeping for X seconds to show parrallelism
class HttpL2Request : public Task {
	public:
		HttpL2Request(librados::L2CacheRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
			pthread_mutex_init(&qmtx,0);
                        pthread_cond_init(&wcond, 0);	
		}
		~HttpL2Request() {
			pthread_mutex_destroy(&qmtx);
                        pthread_cond_destroy(&wcond);
		}
		virtual void run();
		virtual void set_handler(void *handle){
			curl_handle = (CURL *)handle;
		}
	private: 
		int submit_bhttp_request();
	private:
		pthread_mutex_t qmtx;
                pthread_cond_t wcond;
		librados::L2CacheRequest *req;		
		CURL *curl_handle;
		CephContext *cct;
};

#endif

/*engage1*/
