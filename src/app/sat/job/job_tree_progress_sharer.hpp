#pragma once

#include "app/sat/job/base_sat_job.hpp"
#include "comm/job_tree_snapshot.hpp"
#include "comm/msgtags.h"
#include "data/job_transfer.hpp"
#include "util/logger.hpp"
#include <algorithm>
#include <cassert>
#include <string>
#include <utility>
#include <vector>

#define PROGRESS_SHARER_SEPERATOR -1


// class for aggregating and broadcasting progress requests and responses
class JobTreeProgressSharer {

private:
    enum Stage {REQUEST_AGGREGATION, REQUEST_BROADCAST, RESPONSE_AGGREGATION, RESPONSE_BROADCAST, DONE};

public:
    // request structure that is beeing aggregated and broadcastet between all workers
    class Request {
    private:
        std::vector<IntPair> intervals;
        std::vector<int> requests;

    public:

        // default constructor
        Request() = default;

        Request (std::vector<int>& payload) {
            int sep1_idx = -1, sep2_idx = -1;
            for (int i = 0; i < payload.size(); ++i) {
                if (payload[i] != PROGRESS_SHARER_SEPERATOR) {
                    continue;
                } else if (sep1_idx == -1) {
                    sep1_idx = i;
                    continue;
                } else if (sep2_idx == -1) {
                    sep2_idx = i;
                            return;}
            }

            // decode intervals
            assert(sep1_idx % 2 == 0);
            for (int i = 0; i < sep1_idx; i+=2) {
                intervals.push_back(IntPair(
                    payload[i],
                    payload[i+1]
                ));
            }

            // decode requests
            for (int i = sep1_idx+1; i < payload.size(); ++i) {
                requests.push_back(payload[i]);
            }
        }


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
            
            return result;
        }


        // add one interval to the request
        void addInterval(IntPair&& interval) {
            
            // add interval and request
            int pos = sortedBinaryInsert(interval);
            requests.insert(requests.begin() + pos, 1);

            // check overlap with left interval
            if (pos > 0) {
                if (intervals[pos-1].second >= intervals[pos].first) {

                    // merge 2 intervals
                    intervals[pos-1] = IntPair(
                        intervals[pos-1].first,
                        intervals[pos].second
                    );

                    requests[pos-1] += requests[pos];

                    intervals.erase(intervals.begin() + pos);
                    requests.erase(requests.begin() + pos);
                    pos--;
                }
            }

            // check overlap with right interval
            if (pos < intervals.size()-1) {
                if (intervals[pos+1].first <= intervals[pos].second) {

                    // merge 2 intervals
                    intervals[pos+1] = IntPair(
                        intervals[pos].first,
                        intervals[pos+1].second
                    );

                    requests[pos+1] += requests[pos];

                    intervals.erase(intervals.begin() + pos);
                    requests.erase(requests.begin() + pos);
                }
            }
        }

        void merge(Request other) {
            assert(other.intervals.size() == other.requests.size());

            for (int i = 0; i < other.intervals.size(); ++i) {
                addInterval(std::move(other.intervals[i]));
            }
        }

        std::string formatIntervals() {

	        if (intervals.size() == 0)
	            return std::string("[ ]");

            std::string out("[ ");

            auto it = intervals.begin();
            while (it < intervals.end()-1) {
                out += "[";
                out += std::to_string(it->first);
                out += ", ";
                out += std::to_string(it->second);
                out += "), ";
                it++;
            }
            out += "[";
      	    out += std::to_string(it->first);
            out += ", ";
	        out += std::to_string(it->second);
      	    out += ")";
            out += " ]";

            return out;
        }

        std::vector<IntPair>& getIntervals() {
            return intervals;
        }

    private:
        int sortedBinaryInsert(IntPair& interval) {
            // IMPORTANT: assure that list is sorted!!!!

            // append if intervals is empty
            if (intervals.size() == 0) {
                intervals.push_back(std::move(interval));
                return 0;
            }

            // find position of the new element in the vector
            int low = 0, high = intervals.size()-1, pos = 0;
            while (low <= high) {
                int current = (high + low) / 2;

                // check if position is found
                if (intervals[current].first <= interval.first) {
                    if (current == intervals.size() - 1) {
                        pos = current+1;
                        break;
                    } else if (intervals[current+1].first > interval.first) {
                        pos = current+1;
                        break;
                    }
                }

                // update position
                if (intervals[current].first < interval.first) {
                    low = current+1;
                } else {
                    high = current-1;
                }
            }

            // insert element
            // TODO: check if this also works for end of vector!!!
            intervals.insert(intervals.begin() + pos, std::move(interval));

            return pos;
        }
    };

    // response structure that is aggregated and broadcasted along the tree
    class Response {
    private:
        struct Slot {
            IntPair interval;
            std::vector<int> clauses; // TODO: make this a Claue store later!!!
        };

    private:
        std::vector<Slot> slots;

    public:

        // default constructor
        Response() = default;

        Response(const std::vector<IntPair>& intervals, const std::vector<std::vector<int>>& clauses) {
            assert(intervals.size() == clauses.size());

            for (int i = 0; i < intervals.size(); ++i) {
                Slot slot;
                slot.interval = intervals[i];
                
                // add this later when clauses are actually there
                // slot.clauses = clauses[i]; 
                slot.clauses = {};
            }
        }

        Response(std::vector<int>& payload) {
            if (payload.size() == 0) {
                return;
            }

            int current_min = 0;
            int current_max = 0;
            while (true) {
                Slot slot;

                while (payload[current_max] != PROGRESS_SHARER_SEPERATOR)
                    current_max++;

                // decode interval
                slot.interval = IntPair(
                    payload[current_min],
                    payload[current_min+1]
                );

                current_min = current_max+1;
                current_max++;
                while (payload[current_max != PROGRESS_SHARER_SEPERATOR])
                    current_max++;

                // decode clauses
                for (int i = current_min; i < current_max; i++) {
                    slot.clauses.push_back(payload[i]);
                }

                // push back slot
                slots.push_back(std::move(slot));

                if (current_max == payload.size()-1)
                    break;
            }
        }

        std::vector<int> encode() {
            std::vector<int> out;
            for (auto slot : slots) {
                out.push_back(slot.interval.first);
                out.push_back(slot.interval.second);
                out.push_back(PROGRESS_SHARER_SEPERATOR);
                
                for (auto clause : slot.clauses) {
                    out.push_back(clause);
                }
                out.push_back(PROGRESS_SHARER_SEPERATOR);
            }

            return out;
        }

        void merge(Response other) {
            // every response must have the same intervals, even if there are no clauses
            
            assert(slots.size() == other.slots.size());

            for (int i = 0; i < slots.size(); ++i) {
                slots[i] = mergeSlots(std::move(slots[i]), std::move(other.slots[i]));
            }
        }

        std::vector<Slot>& getSlots() {
            return slots;
        }

        std::string formatIntervals() {

            if (slots.size() == 0)
                return std::string("[ ]");

            
            std::string out("[ ");

            auto it = slots.begin();
            while (it < slots.end()-1) {
                out += "[";
                out += std::to_string(it->interval.first);
                out += ", ";
                out += std::to_string(it->interval.second);
                out += "), ";
                it++;
            }
            out += "[";
      	    out += std::to_string(it->interval.first);
            out += ", ";
	        out += std::to_string(it->interval.second);
      	    out += ")";
            out += " ]";

            return out;
        }

    private:
        Slot mergeSlots(Slot&& first, Slot&& second) {
            assert(first.interval.first == second.interval.first);
            assert(first.interval.second == second.interval.second);

            Slot res;
            res.interval = first.interval;

            res.clauses.insert(res.clauses.end(), first.clauses.begin(), first.clauses.end());
            res.clauses.insert(res.clauses.end(), second.clauses.begin(), second.clauses.end());

            return res;
        }
    };


private:
    int _epoch {0};
    Stage _stage {DONE};

    JobTreeSnapshot _treeSnapshot;
    BaseSatJob* _job;

    std::pair<bool, bool> _received_child_elems;
    bool _received_own_elems {false};

    JobMessage _default_msg;

    Request _current_request;
    Response _current_response;

public:
    JobTreeProgressSharer(BaseSatJob* job) :
        _job(job) {
        _default_msg = JobMessage(
            _job->getId(), _job->getActorContextId(), 
            _job->getRevision(), _epoch, MSG_REQUEST_HISTORIC_CLAUSES_AGGREGATE
        );
    }

    // initialize for a new epoch
    void initialize(int epoch) {
        if (_stage != DONE) {
            LOG(V1_WARN, "[WARN] DCS ProgressSharer initialized but not finished!\n");
        }

        // get job tree snapshot
        _treeSnapshot = _job->getJobTree().getSnapshot();

	    LOG(V4_VVER, "DCS progress sharer %i, %i \n", _treeSnapshot.leftChildIndex, _treeSnapshot.rightChildIndex);

        // reset default message
        _epoch = epoch;
        _default_msg = JobMessage(
            _job->getId(), _job->getActorContextId(),
            _job->getRevision(), _epoch, MSG_REQUEST_HISTORIC_CLAUSES_AGGREGATE
        );

        _received_child_elems = std::pair<bool, bool>(
            _treeSnapshot.leftChildIndex == -1,
            _treeSnapshot.rightChildIndex == -1
        );
        _received_own_elems = false;
        _current_request = Request();
        _current_response = Response();

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
                _current_request.merge(request);
                _received_child_elems.first = true;
            }
        } else if (senderId == _treeSnapshot.rightChildIndex) {
            if (_received_child_elems.second) {
                LOG(V1_WARN, "[WARN] DCS request aggregation tried to add child element that is already existent!\n");
            } else {
                _current_request.merge(request);
                _received_child_elems.second = true;
            }
        } else {
            LOG(V1_WARN, "[WARN] DCS request aggregation got request from wrong worker id!\n");
        }
    }

    void broadcastRequest(std::vector<int>& payload, int senderId) {
        if (_stage != REQUEST_BROADCAST) {
            LOG(V1_WARN, "[WARN] DCS request broadcast but in wrong stage!\n");
            return;
        }

        if (senderId != _treeSnapshot.parentIndex && senderId != _treeSnapshot.index) {
            LOG(V1_WARN, "[WARN] DCS revceived request broadcast message from wrong worker %i => %i", senderId, _treeSnapshot.parentIndex);
            return;
        }

        JobMessage& msg = _default_msg;
        msg.payload = payload;
        msg.tag = MSG_REQUEST_HISTORIC_CLAUSES_BROADCAST;
        _treeSnapshot.sendToAnyChildren(msg, MSG_SEND_APPLICATION_MESSAGE);

        _stage = RESPONSE_AGGREGATION;
    }

    void addOwnResponse(Response response) {
        if (_stage != RESPONSE_AGGREGATION) {
            LOG(V1_WARN, "[WARN] DCS response aggregation tried but in wrong stage!\n");
            return;
        }

        _current_response.merge(response);
        _received_own_elems = true;
    }

    void addChildResponse(Response response, int senderId) {
        if (_stage != RESPONSE_AGGREGATION) {
            LOG(V1_WARN, "[WARN] DCS response aggregation received child response but in wrong stage!\n");
            return;
        }

        if (senderId == _treeSnapshot.leftChildIndex) {
            if (_received_child_elems.first) {
                LOG(V1_WARN, "[WARN] DCS response aggregation tried to add child element that is already existent!\n");
            } else {
                _current_response.merge(response);
                _received_child_elems.first = true;
            }
        } else if (senderId == _treeSnapshot.rightChildIndex) {
            if (_received_child_elems.second) {
                LOG(V1_WARN, "[WARN] DCS response aggregation tried to add child element that is already existent!\n");
            } else {
                _current_response.merge(response);
                _received_child_elems.second = true;
            }
        } else {
            LOG(V1_WARN, "[WARN] DCS response aggregation got response from wrong worker id!\n");
        }
    }

    void broadcastResponse(std::vector<int>& payload, int senderId) {
        if (_stage != RESPONSE_BROADCAST) {
            LOG(V1_WARN, "[WARN] DCS response broadcast but in wrong stage!\n");
            return;
        }

        if (senderId != _treeSnapshot.parentIndex && senderId != _treeSnapshot.index) {
            LOG(V1_WARN, "[WARN] DCS revceived response broadcast message from wrong worker %i => %i\n", senderId, _treeSnapshot.parentIndex);
            return;
        }

        JobMessage& msg = _default_msg;
        msg.payload = payload;
        msg.tag = MSG_FORWARD_HISTORIC_CLAUSES_BROADCAST;
        _treeSnapshot.sendToAnyChildren(msg, MSG_SEND_APPLICATION_MESSAGE);

        _stage = DONE;
    }

    // advance aggregation or broadcasting stage and send messages to parent or children respectively
    void advance() {

        // in request aggregation stage and all necessary requests received
        if (_stage == REQUEST_AGGREGATION
            && _received_own_elems
            && _received_child_elems.first
            && _received_child_elems.second
        ) {

            // now in request broadcast stage
            _stage = REQUEST_BROADCAST;

            JobMessage& msg = _default_msg;
            msg.payload = _current_request.encode();

            // if root node, start broadcast
            if (_treeSnapshot.index == 0) {
                LOG(V4_VVER, "DCS request aggregation finished on root node, starting broadcast\n");
                LOG(V4_VVER, "DCS request intervals: %s\n", _current_request.formatIntervals().c_str());

                msg.tag = MSG_REQUEST_HISTORIC_CLAUSES_BROADCAST;
                _treeSnapshot.sendToSelf(msg, MSG_SEND_APPLICATION_MESSAGE);
            } else {
                LOG(V4_VVER, "DCS request aggregation finished on worker %i, sending request to parent\n", _treeSnapshot.index);

                msg.tag = MSG_REQUEST_HISTORIC_CLAUSES_AGGREGATE;
                _treeSnapshot.sendToParent(msg, MSG_SEND_APPLICATION_MESSAGE);
            }

            // reset received elems variables
            _received_own_elems = false;
            _received_child_elems = std::pair<bool, bool>(
                _treeSnapshot.leftChildIndex == -1,
                _treeSnapshot.rightChildIndex == -1
            );

            return;
        }

        // in response aggregation stage and all necessary responses received
        if (_stage == RESPONSE_AGGREGATION
            && _received_own_elems
            && _received_child_elems.first
            && _received_child_elems.second
        ) {

            // now in Response broadcast stage
            _stage = RESPONSE_BROADCAST;

            JobMessage& msg = _default_msg;
            msg.payload = _current_response.encode();

            // if root node, start broadcast
            if (_treeSnapshot.index == 0) {
                LOG(V4_VVER, "DCS response aggregation finished on root node, starting broadcast\n");
                LOG(V4_VVER, "DCS response intervals: [ %s ]", _current_response.formatIntervals().c_str());

                msg.tag = MSG_FORWARD_HISTORIC_CLAUSES_BROADCAST;
                _treeSnapshot.sendToSelf(msg, MSG_SEND_APPLICATION_MESSAGE);
            } else {
                LOG(V4_VVER, "DCS response aggregation finished on worker %i, sending response to parent\n", _treeSnapshot.index);

                msg.tag = MSG_FORWARD_HISTORIC_CLAUSES_AGGREGATE;
                _treeSnapshot.sendToParent(msg, MSG_SEND_APPLICATION_MESSAGE);
            }

            return;
        }
    }
};
