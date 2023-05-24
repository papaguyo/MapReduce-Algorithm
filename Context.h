//
// Created by Guy Achiam on 13/05/2023.
//


#include <pthread.h>
#include <map>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>


#ifndef EX3_CONTEXT_H
#define EX3_CONTEXT_H



class Context{
public:
    const InputVec *inputVec;
    IntermediateVec interVec;
    OutputVec *outputVec;
    const MapReduceClient *clientMap;
    std::vector<pthread_t> threadsVec;
    std::vector<IntermediateVec> vectorsOfInterVec;
    IntermediateVec allPairs;
    std::map<pthread_t, IntermediateVec> *threadsMap;
    Barrier *barrier;
    std::atomic<unsigned long> *elementsInInputVec;
    std::atomic<unsigned long> mappedElems;
    std::atomic<unsigned long> shuffledElems;
    std::atomic<unsigned long> numOfElemsInInterVec;
    std::atomic<unsigned long> numOfReducedElems;
    std::atomic<unsigned long> waitFlag;
    JobState jobState;
    pthread_mutex_t mapMutex;
    pthread_mutex_t reduceMutex;
    pthread_mutex_t outputMutex;
    pthread_mutex_t stateHandlerMutex;


    //constructor, class methods & destructor
    Context(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel);
    void contextSort(pthread_t pid);
    bool comparePairs(IntermediatePair firstPair, IntermediatePair secondPair);
    void changeJobState(stage_t stage);
    void sortPairsVec();
    ~Context();

};

#endif //EX3_CONTEXT_H
