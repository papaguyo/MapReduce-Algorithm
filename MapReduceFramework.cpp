//
// Created by Guy Achiam on 14/05/2023.
//

#include <iostream>
#include <algorithm>
#include "Context.h"



//functions signatures.
void updatePercentage(Context *pContext);


/**
 * Mutex Actions.
 */

/**
 * mutex destroying
 * @param mutex : pthread mutex to destroy.
 */
void destroyMutex(pthread_mutex_t *mutex){
    if(pthread_mutex_destroy(mutex) != EXIT_SUCCESS){
        fprintf(stderr, "system error: mutex failed to be destroyed\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * mutex locking
 * @param mutex : pthread mutex to lock.
 */
void lockMutex(pthread_mutex_t *mutex){
    if(pthread_mutex_lock(mutex) != EXIT_SUCCESS){
        fprintf(stderr, "system error:mutex failed to be locked\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * mutex unlocking
 * @param mutex : pthread mutex to unlock.
 */
void unlockMutex(pthread_mutex_t *mutex){
    if(pthread_mutex_unlock(mutex) != EXIT_SUCCESS){
        fprintf(stderr, "system error:mutex failed to be unlocked\n");
        exit(EXIT_FAILURE);
    }
}



/**
 * functions related to every thread life cycle and that are called from threadLife method.
 */


/**
 * mapping each input pair to the client map.
 * @param context : a pointer to the job context.
 */
void mapStage(Context *context){
    unsigned long oldValue = (*context->elementsInInputVec)++;

    while (oldValue < context->inputVec->size()){
        InputPair inPair = context->inputVec->at(oldValue);
        context->clientMap->map(inPair.first, inPair.second, context);

        //updating the percentage;
        lockMutex(&context->stateHandlerMutex);
        context->mappedElems++;
        updatePercentage(context);
        oldValue = (*context->elementsInInputVec)++;
        unlockMutex(&context->stateHandlerMutex);
    }
}


/**
 * shuffle stage, synchronizing every pair to a specific vector based on its key value.
 * @param context
 */
void shuffleStage(Context *context){
    if(pthread_self() != context->threadsVec.at(0))
        return;
    context->changeJobState(SHUFFLE_STAGE);

    //collecting all InterMediatePair pairs.
    for(auto &currThread : context->threadsVec){
        IntermediateVec curVec = context->threadsMap->at(currThread);
        for(auto &pair : curVec){
            context->allPairs.push_back(pair);
        }
    }

    //sorting the allPairs vector
    context->sortPairsVec();
    //reversing the allPairs vector to get all the greatest pairs to front.
    std::reverse(context->allPairs.begin(), context->allPairs.end());

    //splitting the allPairs vector to vectors by pair key and inserting them in the vectorOfVectors.
    IntermediateVec greatestKeyVec;
    for(const auto &pair : context->allPairs){
        if(!greatestKeyVec.empty() && context->comparePairs(pair, greatestKeyVec.back())){
            context->vectorsOfInterVec.push_back(greatestKeyVec);
            greatestKeyVec.clear();
        }
        greatestKeyVec.push_back(pair);
        updatePercentage(context);
    }
    if(!greatestKeyVec.empty()) {
        context->vectorsOfInterVec.push_back(greatestKeyVec);
    }
}


/**
 * reduce stage, last part of the threadLife method.
 * @param context : a pointer to the job context.
 */
void reduceStage(Context* context){
    unsigned long oldVal = context->numOfElemsInInterVec++;

    while (oldVal < context->vectorsOfInterVec.size()){
        IntermediateVec jobPairs = context->vectorsOfInterVec.at(oldVal);
        context->clientMap->reduce(&jobPairs, context);

        lockMutex(&context->stateHandlerMutex);
        updatePercentage(context);
        oldVal = context->numOfElemsInInterVec++;
        unlockMutex(&context->stateHandlerMutex);
    }
}


/**
 * Updating job complete percentage
 * @param context : a pointer to the job context.
 */
void updatePercentage(Context *context) {
    unsigned long totalNumOfElems;
    unsigned long numOfDoneElems;
    switch (context->jobState.stage) {
        case MAP_STAGE:
            totalNumOfElems = context->inputVec->size();
            numOfDoneElems = context->mappedElems.load();
            break;
        case SHUFFLE_STAGE:
            context->shuffledElems++;
            numOfDoneElems = context->shuffledElems.load();
            totalNumOfElems = context->allPairs.size();
            break;
        case REDUCE_STAGE:
            context->numOfReducedElems++;
            numOfDoneElems = context->numOfReducedElems.load();
            totalNumOfElems = context->vectorsOfInterVec.size();
            break;
        case UNDEFINED_STAGE:
            return;
    }
    if(numOfDoneElems <= totalNumOfElems)
        context->jobState.percentage = ((float)numOfDoneElems/(float)totalNumOfElems) * 100;
}




/**
 * thread main method. every thread cycle through the method stages.
 * @param arg :
 * @return
 */
void *threadLife(void *arg){
    auto *context = static_cast<Context*> (arg);
    //map
    context->changeJobState(MAP_STAGE);
    mapStage(context);
    
    //sort
    context->contextSort(pthread_self());
    context->barrier->barrier();

    //shuffle
    shuffleStage(context);
    context->barrier->barrier();;

    //reduce
    context->changeJobState(REDUCE_STAGE);
    reduceStage(context);

    return nullptr;
}


/**
 * API Methods
 */


/**
 * emit2 loads the job context thread map.
 * @param key
 * @param value
 * @param context
 */
void emit2 (K2* key, V2* value, void* context){
    auto *jobContext = static_cast<Context*> (context);
    lockMutex(&jobContext->mapMutex);
    jobContext->threadsMap->operator[](pthread_self()).push_back(IntermediatePair(key, value));
    unlockMutex(&jobContext->mapMutex);
}

/**
 * emit3 loads the output vec.
 * @param key
 * @param value
 * @param context
 */
void emit3(K3* key, V3* value, void* context){
    auto *jobContext = static_cast<Context*> (context);
    lockMutex(&jobContext->outputMutex);
    jobContext->outputVec->push_back(OutputPair(key, value));
    unlockMutex(&jobContext->outputMutex);
}

/**
 * initial the MapReduce frameWork.
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 * @return
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    return new Context(client, inputVec, outputVec, multiThreadLevel);
}

/**
 * wait method. wating to a certain job to be completed.
 * @param job
 */
void waitForJob(JobHandle job){
    auto *context = static_cast<Context*> (job);
    if(context->waitFlag.load() > 0)
        return;
    else
        context->waitFlag++;

    // threads join process.
    for(auto thread : context->threadsVec){
        if(pthread_join(thread, nullptr) != EXIT_SUCCESS){
            fprintf(stderr, "system error: thread join process failed\n");
            exit(EXIT_FAILURE);
        }
    }
}

/**
 * updating the stage state and percentage to the JobState argument
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState* state){
    auto *context = static_cast<Context*> (job);
    lockMutex(&context->stateHandlerMutex);
    state->stage = context->jobState.stage;
    state->percentage = context->jobState.percentage;
    unlockMutex(&context->stateHandlerMutex);
}

/**
 * closing the job as well as destroying the job context.
 * @param job
 */
void closeJobHandle(JobHandle job){
    auto context = static_cast<Context*> (job);
    waitForJob(job);
    delete context;
}

