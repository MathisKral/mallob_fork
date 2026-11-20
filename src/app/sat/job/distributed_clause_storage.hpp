#pragma once

#include "app/sat/job/base_sat_job.hpp"
#include "app/sat/job/job_tree_progress_sharer.hpp"
#include "util/logger.hpp"
#include "util/sys/background_worker.hpp"
#include "util/sys/threading.hpp"
#include <list>
#include <memory>
#include <vector>

class DistributedClauseStorage {

private:

    // from HCS
    struct BackgroundTask {
        enum Type {INSERT, INSERT_AND_IMPORT, REQUEST} type;
        int epochBegin;
        int epochEnd;
        bool finished {false};

        BackgroundTask(Type type, int epochBegin, int epochEnd) : 
            type(type), epochBegin(epochBegin), epochEnd(epochEnd) {}
    };

    // subclass worker for performing all data and request processing in the background
    class Worker {

    private:
        int max_same_breadth_slots;
        // slot structure containing the clauses for one epoch interval
        // [i, j)
        struct Slot {
            int epochBegin;
            int epochEnd;
            Slot(int epochBegin, int epochEnd, int max_same_breadth_slots) : 
                epochBegin(epochBegin), epochEnd(epochEnd) {}
        };

        std::list<Slot> _storage_list;

        ConditionVariable _cond_var_bg_task;
        Mutex _mtx_bg_task;
        std::list<BackgroundTask> _bg_tasks;
        BackgroundWorker _bg_worker;
        int _num_open_tasks {0};

    public:
        Worker(int max_same_breadth_slots) :
            max_same_breadth_slots(max_same_breadth_slots) {
            _bg_worker.run([this]() {runBackgroundWorker();});
        }

        void addTask(BackgroundTask::Type type, int epochBegin, int epochEnd) {
            // make sure the background worker is not running
            {
                auto lock = _mtx_bg_task.getLock();
                _bg_tasks.emplace_back(type, epochBegin, epochEnd);
            }
            _cond_var_bg_task.notify();
            _num_open_tasks++;
        }

        bool hasFinishedTask() {
            if (_num_open_tasks == 0) return false;
            auto lock = _mtx_bg_task.getLock();
            if (_bg_tasks.empty()) return false;
            return _bg_tasks.front().finished;
        }

        BackgroundTask getFinishedTask() {
            auto lock = _mtx_bg_task.getLock();
            BackgroundTask task = std::move(_bg_tasks.front());
            _bg_tasks.pop_front();
            lock.unlock();
            _cond_var_bg_task.notify();
            _num_open_tasks--;
            return task;
        }

        ~Worker() {
            {
                auto lock = _mtx_bg_task.getLock();
                _bg_worker.stopWithoutWaiting();
            }
            _cond_var_bg_task.notify();
        }

    private:
        void runBackgroundWorker() {
            LOG(V4_VVER, "DCS start background worker\n");

            while(_bg_worker.continueRunning()) {
                
                // prevent busy wait
                _cond_var_bg_task.wait(_mtx_bg_task, 
                    [&]() {
                    return (!_bg_tasks.empty() && !_bg_tasks.back().finished);
                });

                // process unprocessed task
                auto lock = _mtx_bg_task.getLock();
                auto it = _bg_tasks.begin();
                while (it != _bg_tasks.end() && it->finished) ++it;
                if (it == _bg_tasks.end()) continue;
                BackgroundTask& task = *it;
                lock.unlock();

                // process different task types
                switch (task.type) {

                // worker has received a broadcasted request for clauses
                case BackgroundTask::REQUEST:
                    /*
                    TODO
                    */
                    break;

                // worker inserts a new slot after clause sharing
                case BackgroundTask::INSERT:
                    /*
                    TODO
                    */
                    break;

                // worker has received a broadcasted clause response
                case BackgroundTask::INSERT_AND_IMPORT:
                    /*
                    TODO
                    */
                    break;
                }
                task.finished = true;
            }
        }
    };

// actual fields
private:
const int _max_same_breadth_slots {1};

BaseSatJob *_job;
Worker _worker;
std::unique_ptr<JobTreeProgressSharer> _sharer;

int _last_epoch {0};
std::vector<std::pair<int, int>> _missing_epoch_intervals;

public:
    DistributedClauseStorage(BaseSatJob* job, int max_same_breadth_slots) : 
         _max_same_breadth_slots(max_same_breadth_slots), _job(job), _worker(_max_same_breadth_slots) {
            LOG(V4_VVER, "DCS Setup Distributed Clause Storage\n");
            _sharer.reset(new JobTreeProgressSharer(_job));
        }

    // reset progress sharer to current clause sharing epoch
    void resetProgressSharer(int epoch) {
	LOG(V4_VVER, "DCS reset progress sharer at epoch %i\n", epoch);
        _sharer->initialize(epoch);
    }


    // import clauses from clause sharing
    // gets called during every clause sharing iteration
    void importSharing(int epoch, std::vector<int>&& clauses) {
        LOG(V5_DEBG, "DCS import clauses\n");

        updateMissingIntervals(epoch, epoch+1);

        // fast-forward slot creation for empty slots
        // TODO: make this create optimal logarithmic slots later
        while (_last_epoch < epoch-1) {
            _last_epoch++;
            LOG(V4_VVER, "DCS insert [%i,%i) empty\n", _last_epoch, _last_epoch+1);
            // TODO: worker.addTask...
        }

        // insert new set of clauses received from clause sharing
        LOG(V4_VVER, "DCS insert [%i,%i) buflen=...\n", epoch, epoch+1);
        // TODO: worker.addTask...
        _last_epoch = epoch;

        // finally request progress if missing
        requestMissingInterval();
    }

    // handle the request for a interval
    void handleIntervalRequestAggregation(int epoch, int senderId, JobTreeProgressSharer::Request request) {
        LOG(V4_VVER, "DCS received request aggregation for epoch %i from worker %i\n", epoch, senderId);
        
        // add request to progress sharer
        _sharer->addChildRequest(request, senderId);
        _sharer->advance();
    }

    void handleIntervalRequestBroadcast(int epoch, int senderId, std::vector<int>& payload) {
        LOG(V4_VVER, "DCS received request broadcast for epoch %i\n", epoch);
        
        // advance broadcasting
        _sharer->broadcastRequest(payload, senderId);

        // dont call `_sharer->advance() here because it is called when (1) an aggregation message comes in and`
        // (2) when the worker finished preparing the clauses 

        // ask worker to prepare clauses
        /*
        TODO: worker.addTask...
        */
    }

    void handleIntervalResponseAggregation(/*TODO*/) {
        LOG(V4_VVER, "DCS received response aggregation for interval [...,...)\n");
        // TODO: worker.addTask...
    }

    void handleIntervalResponseBroadcast(/*TODO*/) {
        LOG(V4_VVER, "DCS received response broadcast for interval [...,...)\n");
        // TODO: worker.addTask...
    }


    // process all finished tasks from the worker
    void handleFinishedTasks() {
        //LOG(V4_VVER, "DCS handle finished worker tasks\n");
        //TODO: worker.addTask...
    }

private:

    // update missing intervals based on incoming interval
    // if an incoming interval creates a hole, add that hole to the missing intervals list
    // -- HCS implementation: request the largest interval possible
    // -- TODO later: request the optimal logarithmic intervals
    void updateMissingIntervals(int epochBegin, int epochEnd) {
    
        // does this close an existing hole?
        for (auto it = _missing_epoch_intervals.begin(); it != _missing_epoch_intervals.end(); ++it) {
            auto& [iBegin, iEnd] = *it;
            if (epochBegin <= iBegin && epochEnd >= iEnd) {
                // full match: delete missing interval
                LOG(V4_VVER, "HCS close full hole\n");
                it = _missing_epoch_intervals.erase(it);
                --it;
            } else if (epochBegin <= iBegin && epochEnd > iBegin && epochEnd < iEnd) {
                // left-end match: reduce missing interval to its right part
                LOG(V4_VVER, "HCS close hole left\n");
                iBegin = epochEnd;
            } else if (epochBegin > iBegin && epochBegin < iEnd && epochEnd >= iEnd) {
                // right-end match: reduce missing interval to its left part
                LOG(V4_VVER, "HCS close hole right\n");
                iEnd = epochBegin;
            } else if (iBegin < epochBegin && epochEnd < iEnd) {
                // match in the middle: separate interval into two smaller ones
                LOG(V4_VVER, "HCS close hole mid\n");
                std::pair<int, int> secondInterval = {epochEnd, iEnd};
                // insert first new interval
                it = _missing_epoch_intervals.insert(it, std::pair<int, int>(iBegin, epochBegin));
                // go to second (existing) interval
                ++it;
                // overwrite with new bounds
                *it = secondInterval;
            }
        }

        // are we creating a new hole between the last epoch and the (first) received epoch?
        if (epochBegin > _last_epoch+1) {
            // add to missing epoch intervals
            if (!_missing_epoch_intervals.empty() && _missing_epoch_intervals.back().second == epochBegin-1) {
                // add to existing interval
                LOG(V4_VVER, "HCS extend hole\n");
                _missing_epoch_intervals.back().second++;
            } else {
                // add new interval
                LOG(V4_VVER, "HCS add hole\n");
                _missing_epoch_intervals.emplace_back(_last_epoch+1, epochBegin);
            }
        }

        printMissingIntervals();
    }

    void printMissingIntervals() {
        std::string missingIntervals;
        for (auto& [begin, end] : _missing_epoch_intervals) 
            missingIntervals += "[" + std::to_string(begin) + "," + std::to_string(end) + ") ";
        if (!missingIntervals.empty())
            LOG(V4_VVER, "HCS missing %s\n", missingIntervals.c_str());
    }

    void requestMissingInterval() {

        // request a missing interval
        if (!_missing_epoch_intervals.empty()) {
            auto [intervBegin, intervEnd] = _missing_epoch_intervals.front();
            LOG(V4_VVER, "DCS requesting [%i, %i) in epoch %i\n", intervBegin, intervEnd, _last_epoch);
            
            // add missing interval to request and advance request
            JobTreeProgressSharer::Request req;
            req.addInterval(std::pair<int, int>(intervBegin, intervEnd), _job->getJobTree().getIndex());
            _sharer->addOwnRequest(req);
            _sharer->advance();

        // no interval to request, send empty message for aggregation
        } else {
            LOG(V4_VVER, "DCS empty interval request in epoch %i\n", _last_epoch);
            
            // add default request and advance
            JobTreeProgressSharer::Request req;
            _sharer->addOwnRequest(req);
            _sharer->advance();
        }
    }
};
