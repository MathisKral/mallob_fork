#pragma once // ifndef ??

#include "app/sat/data/clause.hpp"
#include "app/sat/data/produced_clause_candidate.hpp"
#include "app/sat/job/base_sat_job.hpp"
#include "app/sat/job/generic_clause_storage.hpp"
#include "app/sat/sharing/buffer/buffer_builder.hpp"
#include "app/sat/sharing/buffer/buffer_reader.hpp"
#include "app/sat/sharing/filter/exact_clause_filter.hpp"
#include "app/sat/sharing/filter/generic_clause_filter.hpp"
#include "app/sat/sharing/store/generic_clause_store.hpp"
#include "app/sat/sharing/store/static_clause_store.hpp"
#include "comm/msgtags.h"
#include "data/job_transfer.hpp"
#include "util/logger.hpp"
#include "util/params.hpp"
#include "util/sys/background_worker.hpp"
#include "util/sys/threading.hpp"
#include <cmath>
#include <cstdint>
#include <limits>
#include <list>
#include <memory>
#include <utility>
#include <vector>

class DuplicatedClauseStorage : public GenericClauseStorage {

private:
    class Worker {

    private:
        StaticClauseStore<false> _cdb; // verify this later but shouldn't be concurrent

        std::unique_ptr<GenericClauseStore> _filter_store; // dummy for the filter
        std::unique_ptr<ExactClauseFilter> _filter;

        ConditionVariable _cond_var_bg_tasks;
        Mutex _mtx_bg_tasks;
        std::list<BackgroundTask> _bg_tasks;
        BackgroundWorker _bg_worker;

        int _num_open_tasks {0};

    public:
        Worker(const Parameters& params) : _cdb(params, false, 265, true, INT32_MAX),
            _filter_store(new StaticClauseStore<false>(params, false, 256, false, INT32_MAX)),
            _filter(new ExactClauseFilter(*_filter_store, std::numeric_limits<int>::max(), 20)) { // hardcoded from clause_sharing_session for now
            _bg_worker.run([this]() {runBackgroundWorker();});
        } 

        void addTask(BackgroundTask::Type type, int epoch, std::vector<int>&& clauses, bool fromLeftChild) {
            {
                auto lock = _mtx_bg_tasks.getLock();
                _bg_tasks.emplace_back(type, epoch, epoch, std::move(clauses), fromLeftChild);
            }
            _cond_var_bg_tasks.notify();
            _num_open_tasks++;
        }

        bool hasFinishedTask() {
            if (_num_open_tasks == 0) return false;
            auto lock = _mtx_bg_tasks.getLock();
            if (_bg_tasks.empty()) return false;
            return _bg_tasks.front().finished;
        }

        BackgroundTask getFinishedTask() {
            auto lock = _mtx_bg_tasks.getLock();
            BackgroundTask task = std::move(_bg_tasks.front());
            _bg_tasks.pop_front();
            lock.unlock();
            _cond_var_bg_tasks.notify();
            _num_open_tasks--;
            return task;
        }

        ~Worker() {
            {
                auto lock = _mtx_bg_tasks.getLock();
                _bg_worker.stopWithoutWaiting();
            }
            _cond_var_bg_tasks.notify();
        }

    private:
        void runBackgroundWorker() {
            while(_bg_worker.continueRunning()) {
                _cond_var_bg_tasks.wait(_mtx_bg_tasks, 
                    [&]() {
                        return !_bg_worker.continueRunning() || (!_bg_tasks.empty() && !_bg_tasks.back().finished);
                    }
                );

                // Try to find an unprocessed task
                auto lock = _mtx_bg_tasks.getLock();
                auto it = _bg_tasks.begin();
                while (it != _bg_tasks.end() && it->finished) ++it;
                if (it == _bg_tasks.end()) continue;
                BackgroundTask& task = *it;
                lock.unlock();

                if (task.type == BackgroundTask::REQUEST) {
                    processRequestTask(task);
                } else {
                    processInsertTask(task);
                }
                addStorageDiagnosticsToTask(task);
                task.finished = true;
            }
        }

        void processRequestTask(BackgroundTask& task) {
            // send back the buffer
            task.clauses = _cdb.readBuffer();
        }

        void processInsertTask(BackgroundTask& task) {
            // add clauses to database
            auto [accepted, total] = addClausesIntoDatabase(_cdb, task.clauses, task.epochBegin, task.type == BackgroundTask::INSERT_AND_IMPORT);
            LOG(V4_VVER, "DCS added %i/%i clauses into the database\n", accepted, total);
        }

        void addStorageDiagnosticsToTask(BackgroundTask& task) {
            task.storageDiagnostics.numClausesInStorage = _filter->size(0);
            task.storageDiagnostics.numLitsInStorage = _cdb.getCurrentlyUsedLiterals();
            task.storageDiagnostics.slotLayout = "-";
        }


        std::pair<int, int> addClausesIntoDatabase(StaticClauseStore<false>& cdb, std::vector<int>& clauses, int epoch, bool is_import) {
            cdb.setClauseDeletionCallback([&](Mallob::Clause& cls) {
                ProducedClauseCandidate pcc(cls.begin, cls.size, cls.lbd, 0, epoch);
                _filter->erase(std::move(pcc));
            });

            // iterate over all clausaes to be added
            BufferReader reader = cdb.getBufferReader(clauses.data(), clauses.size());
            BufferBuilder builder = cdb.getBufferBuilder(INT32_MAX);

            Mallob::Clause cls = reader.getNextIncomingClause();
            int numAccepted = 0;
            int numTotal = 0;
            while (cls.begin != nullptr) {
                ProducedClauseCandidate pcc(cls.begin, cls.size, cls.lbd, 0, epoch);
                auto result = _filter->tryRegisterAndInsert(std::move(pcc), &cdb);
                if (result == GenericClauseFilter::ADMITTED) {
                    numAccepted++;
                    if (is_import) builder.append(cls);
                }
                numTotal++;
                cls = reader.getNextIncomingClause();
            }

            cdb.clearClauseDeletionCallbacks();

            // import the clauses
            if (is_import) clauses = builder.extractBuffer();

            return std::pair<int, int>(numAccepted, numTotal);
        }
    };

private:
    BaseSatJob* _job;
    Worker _worker;

    int _last_shared_epoch {-1};

public:
    DuplicatedClauseStorage(const Parameters& params, BaseSatJob* job) :
        _job(job), _worker(params) {
            log(V4_VVER, "DCS initialized with\n");
        }

    void importSharing(int epoch, std::vector<int>&& clauses) {
        LOG(V4_VVER, "DCS insert [%i, %i) buflen=%i\n", epoch, epoch+1, clauses.size());
        _worker.addTask(BackgroundTask::INSERT, epoch, std::move(clauses), false);

        // request database if missing
        if (_last_shared_epoch != epoch - 1) {
            _last_shared_epoch = epoch - 1; // for the correct request message
            requestMissingStorage();
        }

        // update epoch counter
        _last_shared_epoch = epoch;
    }

    void handleIncomingMissingInterval(int epochBegin, int epochEnd, std::vector<int>&& clauses) {
        LOG(V4_VVER, "DCS receiving clauses from parent worker, buflen=%i", clauses.size());
        _worker.addTask(BackgroundTask::INSERT_AND_IMPORT, epochEnd, std::move(clauses), false);
    }

    void handleRequestMissingInterval(bool leftChild, int epochBegin, int epochEnd) {
        LOG(V4_VVER, "DCS got request for database at epoch %i", epochEnd);
        _worker.addTask(BackgroundTask::REQUEST, epochEnd, std::vector<int>(), leftChild);
    }

    void handleFinishedTasks() {
        while (_worker.hasFinishedTask()) {
            auto task = _worker.getFinishedTask();

            if (task.type == BackgroundTask::REQUEST) {
                LOG(V4_VVER, "DCS database prepared for child at epoch %i\n", task.epochEnd);
                JobMessage msg(_job->getId(), _job->getContextId(), _job->getRevision(), task.epochBegin, MSG_FORWARD_HISTORIC_CLAUSES);
                msg.payload = std::move(task.clauses);
                msg.payload.push_back(task.epochEnd);
                if (task.fromLeftChild && _job->getJobTree().hasLeftChild()) {
                    _job->getJobTree().sendToLeftChild(msg);
                } else if (_job->getJobTree().hasRightChild()) {
                    _job->getJobTree().sendToRightChild(msg);
                }
            } else if (task.type == BackgroundTask::INSERT_AND_IMPORT) {
                LOG(V4_VVER, "DCS digest historic clauses, buflen=%i", task.clauses.size());
                _job->digestHistoricClauses(task.epochBegin, task.epochEnd, std::move(task.clauses));
            }

            LOG(V4_VVER, "DCS %i cls / %i lits\n", task.storageDiagnostics.numClausesInStorage, task.storageDiagnostics.numLitsInStorage);
        }
    }

private:
    void requestMissingStorage() {
        LOG(V4_VVER, "DCS requesting database from parent\n");
        JobMessage msg(_job->getId(), _job->getContextId(), _job->getRevision(), _last_shared_epoch, MSG_REQUEST_HISTORIC_CLAUSES);
        msg.payload = {_last_shared_epoch, _last_shared_epoch};
        _job->getJobTree().sendToParent(msg);
    }
};