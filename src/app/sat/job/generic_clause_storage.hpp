
#pragma once

#include <vector>

#define MALLOB_CLAUSE_STORAGE_NONE -1
#define MALLOB_CLAUSE_STORAGE_HCS   0
#define MALLOB_CLAUSE_STORAGE_DCS   1

class GenericClauseStorage {

public:
    virtual void importSharing(int epoch, std::vector<int>&& clauses) {};
    virtual void handleIncomingMissingInterval(int epochBegin, int epochEnd, std::vector<int>&& clauses) {};
    virtual void handleRequestMissingInterval(bool leftChild, int epochBegin, int epochEnd) {};
    virtual void handleFinishedTasks() {};
};