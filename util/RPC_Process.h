//
// Created by ruihong on 8/22/23.
//

#ifndef TIMBERSAW_RPC_PROCESS_H
#define TIMBERSAW_RPC_PROCESS_H

class RPC_Process {
public:
    virtual void persistence_unpin_handler(void* arg) {}
    virtual void bucket_meta_update_handler(void* request) = 0;
};

#endif  // TIMBERSAW_RPC_PROCESS_H
