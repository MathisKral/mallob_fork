#pragma once

#include "app/sat/job/base_sat_job.hpp"
#include "comm/job_tree_snapshot.hpp"
#include "comm/msgtags.h"
#include "data/job_transfer.hpp"
#include "util/logger.hpp"
#include <list>
#include <utility>
#include <vector>

#define PROGRESS_SHARER_SEPERATOR -1


// class for aggregating and broadcasting progress requests and responses
class JobTreeProgressSharer {

private:
    enum Stage {REQUEST_AGGREGATION, REQUEST_BROADCAST, RESPONSE_AGGREGATION, RESPONSE_BROADCAST, DONE};

public:
    // request struct that is beeing aggregated and broadcastet between all workers
    class Request{
    private:
        std::list<std::pair<int, int>> intervals;
        std::vector<int> requests;
        std::vector<int> workers;

    public:
        std::vector<int> encode() {
            std::vector<int> result;

            // encode intervals
            for (auto [intervBegin, intervEnd] : intervals) {
                result.push_back(intervBegin);
                result.push_back(intervEnd);
            }
            result.push_back(PROGRESS_SHARER_SEPERATOR);

            // encode number of requests for each interval
            for (auto req : requests) {
                result.push_back(req);
            }
            result.push_back(PROGRESS_SHARER_SEPERATOR);

            // encode worker list
            for (auto worker : workers) {
                result.push_back(worker);
            }
            
            return result;
        }

        static Request decode(std::vector<int> payload) {
            Request request;
            int sep1_idx = -1, sep2_idx = -1;
            for (int i = 0; i < payload.size(); ++i) {
                if (payload[i] != PROGRESS_SHARER_SEPERATOR) {
                    continue;
                } else if (sep1_idx == -1) {
                    sep1_idx = i;
                    continue;
                } else if (sep2_idx == -1) {
                    sep2_idx = i;
                }
            }

            // decode intervals
            assert(sep1_idx % 2 == 0);
            for (int i = 0; i < sep1_idx; i+=2) {
                request.intervals.push_back(std::pair<int, int>(
                    payload[i],
                    payload[i+1]
                ));
            }

            // decode requests
            for (int i = sep1_idx+1; i < sep2_idx; ++i) {
                request.requests.push_back(payload[i]);
            }

            // decode worker list
            for (int i = sep2_idx+1; i < payload.size(); ++i) {
                request.workers.push_back(payload[i]);
            }

            return request;
        }

        // add one interval to the request
        void addInterval(std::pair<int, int> interval, int worker) {
            // FIXME: only temporary
            // the intervals list has to be sorted!!!!
            intervals.push_back(interval);
            requests.push_back(1);
            workers.push_back(worker);
        }

        void merge(Request other) {
            // FIXME: only temporary
            // the intervals have to be merged and sorted!!!!
            for (auto interval : other.intervals) {
                intervals.push_back(interval);
            }
            for (auto request : other.requests) {
                requests.push_back(request);
            }
            for (auto worker : other.workers) {
                workers.push_back(worker);
            }
        }
    };


private:
    int _epoch {0};
    Stage _stage {DONE};

    JobTreeSnapshot _treeSnapshot;
    BaseSatJob* _job;

    std::pair<bool, bool> _received_child_elems;
    bool _received_own_elems {false};

    Request _default_req;
    JobMessage _default_msg;

    Request _current_request;

public:
    JobTreeProgressSharer(BaseSatJob* job) :
        _job(job) {
        _default_msg = JobMessage(
            _job->getId(), _job->getActorContextId(), 
            _job->getRevision(), _epoch, MSG_REQUEST_HISTORIC_CLAUSES_AGGREGATE
        );
        _default_msg.payload = _default_req.encode();
    }

    // initialize for a new epoch
    void initialize(int epoch, const JobTreeSnapshot& treeSnapshot) {
        if (_stage != DONE) {
            LOG(V1_WARN, "[WARN] DCS ProgressSharer initialized but not finished!\n");
        }

	LOG(V4_VVER, "DCS progress sharer %i, %i \n", _treeSnapshot.leftChildIndex, _treeSnapshot.rightChildIndex);

        _epoch = epoch;
        _treeSnapshot = treeSnapshot;

        _received_child_elems = std::pair<bool, bool>(
            _treeSnapshot.leftChildIndex == -1,
            _treeSnapshot.rightChildIndex == -1
        );
        _received_own_elems = false;
        _current_request = Request();

        _stage = REQUEST_AGGREGATION;
    }


    void addOwnRequest(Request request) {
        if (_stage != REQUEST_AGGREGATION) {
            LOG(V1_WARN, "[WARN] DCS request aggregation tried to add request but was in wrong stage!\n");
        } else {
            LOG(V4_VVER, "DCS adding own workers request\n");
        }

        _current_request.merge(request);
        _received_own_elems = true;
    }

    void addChildRequest(Request request, int senderId) {
        if (_stage != REQUEST_AGGREGATION) {
            LOG(V1_WARN, "[WARN] DCS request aggregation tried to add request but was in wrong stage!\n");
        }

        // check senderId
        if (senderId == _treeSnapshot.leftChildIndex) {
            if (_received_child_elems.first) {
                LOG(V1_WARN, "[WARN] DCS request aggregation tried to add child element that is already existent!\n");
            } else {
		LOG(V4_VVER, "DCS progress aggregation received request from worker %i\n", senderId);
                _current_request.merge(request);
                _received_child_elems.first = true;
            }
        } else if (senderId == _treeSnapshot.rightChildIndex) {
            if (_received_child_elems.second) {
                LOG(V1_WARN, "[WARN] DCS request aggregation tried to add child element that is already existent!\n");
            } else {
		LOG(V4_VVER, "DCS progress aggregation received request from worker %i\n", senderId);
                _current_request.merge(request);
                _received_child_elems.second = true;
            }
        } else {
            LOG(V1_WARN, "[WARN] DCS request aggregation got request from wrong worker id!\n");
        }
    }

    // advance aggregation or broadcasting stage and send messages to parent or children respectively
    void advance() {
        if (_stage == REQUEST_AGGREGATION
            && _received_own_elems
            && _received_child_elems.first
            && _received_child_elems.second
        ) {

            JobMessage& msg = _default_msg;
            msg.payload = _current_request.encode();

            // if root node, start broadcast
            if (_treeSnapshot.index == 0) {
                LOG(V4_VVER, "DCS request aggregation finished on root node, starting broadcast\n");

                _treeSnapshot.sendToAnyChildren(msg, MSG_REQUEST_HISTORIC_CLAUSES_BROADCAST);
                _stage = RESPONSE_AGGREGATION;
            } else {
                LOG(V4_VVER, "DCS request aggregation finished on worker %i, sending request to parent\n", _treeSnapshot.index);

                _treeSnapshot.sendToParent(msg, MSG_REQUEST_HISTORIC_CLAUSES_AGGREGATE);
                _stage = REQUEST_BROADCAST;
            }
        }
    }
};
