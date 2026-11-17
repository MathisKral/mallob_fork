#pragma once

#include "comm/job_tree_snapshot.hpp"
#include "util/logger.hpp"
#include <utility>


// class for aggregating and broadcasting progress requests and responses
class JobTreeProgressSharer {

private:
    enum Stage {REQUEST_AGGREGATION, REQUEST_BROADCAST, RESPONSE_AGGREGATION, RESPONSE_BROADCAST, DONE};

private:
    int _epoch {0};
    int _workerId;
    Stage _stage {DONE};
    JobTreeSnapshot _treeSnapshot;
    std::pair<bool, bool> _received_child_elems;
    std::pair<int, int> _expected_child_ids;
    JobMessage _default_msg;

public:
    JobTreeProgressSharer(int _workerId, const JobTreeSnapshot& treeSnapshot) :
        _workerId(_workerId), _treeSnapshot(treeSnapshot) {
            _expected_child_ids = std::pair<int, int>(
                _treeSnapshot.leftChildIndex,
                _treeSnapshot.rightChildIndex
            );

            /*
            TODO: _default_msg
            */
    }

    // initialize for a new epoch
    void initialize(int epoch, const JobTreeSnapshot& treeSnapshot) {
        if (_stage != DONE) {
            LOG(V1_WARN, "DCS ProgressSharer initialized but not finished!\n");
        }

        /*
        TODO reset all stuff
        */
    }
};