
#pragma once

#include <string>
#include <vector>

#define MALLOB_CLAUSE_STORAGE_HCS 0
#define MALLOB_CLAUSE_STORAGE_DCS 1
#define MALLOB_CLAUSE_STORAGE_SCS 2

class GenericClauseStorage {

protected:
    struct StorageDiagnostics {
        int numLitsInStorage;
        int numClausesInStorage;
        std::string slotLayout;
    };

    struct BackgroundTask {
        enum Type {INSERT, INSERT_AND_IMPORT, REQUEST} type;
        int epochBegin;
        int epochEnd;
        bool fromLeftChild {false};
        std::vector<int> clauses;
        bool finished {false};
        StorageDiagnostics storageDiagnostics;

        BackgroundTask(Type type, int epochBegin, int epochEnd, const std::vector<int>& clauses, bool fromLeftChild) : 
            type(type), epochBegin(epochBegin), epochEnd(epochEnd), 
            fromLeftChild(fromLeftChild), clauses(clauses) {}
        BackgroundTask(Type type, int epochBegin, int epochEnd, std::vector<int>&& clauses, bool fromLeftChild) : 
            type(type), epochBegin(epochBegin), epochEnd(epochEnd), 
            fromLeftChild(fromLeftChild), clauses(std::move(clauses)) {}
    };

public:
    virtual void importSharing(int epoch, std::vector<int>&& clauses) {};
    virtual void handleIncomingMissingInterval(int epochBegin, int epochEnd, std::vector<int>&& clauses) {};
    virtual void handleRequestMissingInterval(bool leftChild, int epochBegin, int epochEnd) {};
    virtual void handleFinishedTasks() {};
};