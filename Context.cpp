//
// Created by Guy Achiam on 14/05/2023.
//

#include <iostream>
#include "Context.h"
#include "Barrier.h"
#include <algorithm>


//functions signatures.
void lockMutex(pthread_mutex_t *mutex);
void unlockMutex(pthread_mutex_t *mutex);
void destroyMutex(pthread_mutex_t *mutex);
void *threadLife(void *arg);

/**
 * constructor.
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 */
Context::Context(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel):
    threadsVec(multiThreadLevel, 0), clientMap(&client), vectorsOfInterVec(), allPairs()
{
    this->inputVec = &inputVec;
    this->outputVec = &outputVec;
    this->barrier = new Barrier(multiThreadLevel);
    this->threadsMap = new std::map<pthread_t, IntermediateVec>();
    this->elementsInInputVec = new std::atomic<unsigned long>(0);
    this->mappedElems = 0;
    this->shuffledElems = 0;
    this->numOfElemsInInterVec = 0;
    this->numOfReducedElems = 0;
    this->waitFlag = 0;
    this->jobState = {UNDEFINED_STAGE, 0};
    this->mapMutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    this->reduceMutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    this->outputMutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    this->stateHandlerMutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;

    //spawning each thread by calling pthread create method.
    for(int i = 0; i < multiThreadLevel; ++i){
        if(pthread_create(&this->threadsVec.at(i), nullptr, threadLife, this) != EXIT_SUCCESS){
            std::cerr << "system error: pthread failed to be created\n";
            exit(EXIT_FAILURE);
        }
    }

}

/**
 * compare function between pairs.
 * @param firstPair
 * @param secondPair
 * @return
 */
bool Context::comparePairs(IntermediatePair firstPair, IntermediatePair secondPair) {
    return *(firstPair.first) < *(secondPair.first);
}

/**
 * in class compare function.
 * @param firstPair
 * @param secondPair
 * @return
 */
bool compare(IntermediatePair firstPair, IntermediatePair secondPair){
    return *(firstPair.first) < *(secondPair.first);
}

/**
 * sort function for the allPairs vector instance.
 */
void Context::sortPairsVec(){
    lockMutex(&this->mapMutex);
    std::sort(this->allPairs.begin(), this->allPairs.end(), compare);
    unlockMutex(&this->mapMutex);
}

/**
 * sort function, part of every thread cycle.
 */
void Context::contextSort(pthread_t pid) {
    lockMutex(&this->mapMutex);
    std::sort((this->threadsMap)->operator[](pid).begin(),
              (this->threadsMap)->operator[](pid).end(), compare);
    unlockMutex(&this->mapMutex);
}

/**
 * changing the job state.
 * @param stage : state to change for.
 */
void Context::changeJobState(const stage_t stage) {
    lockMutex(&this->stateHandlerMutex);
    if(this->jobState.stage == stage){
        unlockMutex(&this->stateHandlerMutex);
        return;
    }
    this->jobState.stage = stage;
    this->jobState.percentage = 0;
    unlockMutex(&this->stateHandlerMutex);
}

/**
 * destructor.
 */
Context::~Context() {
    delete this->barrier;
    delete this->threadsMap;
    delete this->elementsInInputVec;
    destroyMutex(&this->mapMutex);
    destroyMutex(&this->reduceMutex);
    destroyMutex(&this->outputMutex);
    destroyMutex(&this->stateHandlerMutex);
}
