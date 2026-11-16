#pragma once

#include "app/sat/job/base_sat_job.hpp"
#include "util/logger.hpp"
#include <vector>

class DistributedClauseStorage {

// subclass worker for performing all data and request processing in the background
private:
    class Worker {

    };

// actual fields
private:
BaseSatJob *_job;
Worker _worker;

int _last_epoch {0};
std::vector<std::pair<int, int>> _missing_epoch_intervals;

public:
    DistributedClauseStorage(BaseSatJob* job) : 
        _job(job), _worker() {
            LOG(V4_VVER, "DCS Setup Distributed Clause Storage\n");
        }


    // import clauses from clause sharing
    void importSharing(int epoch, std::vector<int>&& clauses) {
        LOG(V5_DEBG, "DCS import clauses\n");
    }


    // handle a incoming interval
    void handleIncomingMissingInterval(int epochBegin, int epochEnd, std::vector<int>&& clauses) {
        LOG(V5_DEBG, "DCS handle incoming interval [%i, %i)\n", epochBegin, epochEnd);
    }


    // handle the request for a interval
    void handleRequestMissingInterval(int epochBegin, int epochEnd) {
        LOG(V5_DEBG, "DCS received request for interval [%i, %i)\n", epochBegin, epochEnd);
    }


    // process all finished tasks from the worker
    void handleFinishedTasks() {
        LOG(V5_DEBG, "DCS handle finished worker tasks\n");
    }
};