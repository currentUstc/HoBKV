#include <fstream>
#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

// namespace TimberSaw{
int main(int argc, char* argv[]) {
    TimberSaw::Memory_Node_Keeper* mn_keeper;
    int compaction_threads = 12;
    int message_workers = 8;

    if (argc == 4 || argc == 5 || argc == 6) {
        uint32_t tcp_port;
        int pr_size;
        int Memory_server_id;
        char* value = argv[1];
        std::stringstream strValue1;
        strValue1 << value;
        strValue1 >> tcp_port;
        value = argv[2];
        std::stringstream strValue2;
        //  strValue.str("");
        strValue2 << value;
        strValue2 >> pr_size;
        value = argv[3];
        std::stringstream strValue3;
        //  strValue.str("");
        strValue3 << value;
        strValue3 >> Memory_server_id;
        if (argc >= 5) {
            std::stringstream strValue4;
            strValue4 << argv[4];
            strValue4 >> compaction_threads;
            message_workers = compaction_threads;
            if (compaction_threads < 1) {
                std::cerr << "compaction_threads must be positive" << std::endl;
                return 2;
            }
        }
        if (argc == 6) {
            std::stringstream strValue5;
            strValue5 << argv[5];
            strValue5 >> message_workers;
            if (message_workers < 1) {
                std::cerr << "message_workers must be positive" << std::endl;
                return 2;
            }
        }
        const std::string worker_count = std::to_string(message_workers);
        setenv("DLSM_MESSAGE_WORKERS", worker_count.c_str(), 1);
        mn_keeper = new TimberSaw::Memory_Node_Keeper(true, tcp_port, pr_size);
        TimberSaw::RDMA_Manager::node_id = 2 * Memory_server_id;
    } else {
        mn_keeper = new TimberSaw::Memory_Node_Keeper(true, 19843, 88);
        TimberSaw::RDMA_Manager::node_id = 0;
    }

    mn_keeper->SetBackgroundThreads(compaction_threads, TimberSaw::ThreadPoolType::CompactionThreadPool);
    std::cout << "Configured compaction_threads=" << compaction_threads
              << " message_workers=" << message_workers << std::endl;
    //  std::thread CPU_utilization_heartbeat([&](){
    //    // std::ofstream myfile;
    //    while (1){
    //      std::this_thread::sleep_for(std::chrono::milliseconds(CPU_UTILIZATION_CACULATE_INTERVAL));
    //      long double new_percent = TimberSaw::Memory_Node_Keeper::rdma_mg->rpter.getCurrentValue();
    //      sleep(2);
    ////      printf("CPU utilization is %Lf\n", TimberSaw::Memory_Node_Keeper::rdma_mg->rpter.getCurrentValue());
    //    }
    //  });
    //  CPU_utilization_heartbeat.detach();
    mn_keeper->Server_to_Client_Communication();

    delete mn_keeper;

    return 0;
}
