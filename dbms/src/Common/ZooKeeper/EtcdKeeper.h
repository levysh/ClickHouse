#pragma once
 
#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>
 
#include <Poco/Timespan.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <grpcpp/grpcpp.h>
#include <Common/ZooKeeper/rpc.grpc.pb.h>
 
using etcdserverpb::PutRequest;
using etcdserverpb::PutResponse;
using etcdserverpb::DeleteRangeRequest;
using etcdserverpb::DeleteRangeResponse;
using etcdserverpb::RangeRequest;
using etcdserverpb::RangeResponse;
using etcdserverpb::RequestOp;
using etcdserverpb::ResponseOp;
using etcdserverpb::TxnRequest;
using etcdserverpb::TxnResponse;
using etcdserverpb::Compare;
using etcdserverpb::KV;
using etcdserverpb::Watch;
// using etcdserverpb::WatchRequest;
// using etcdserverpb::WatchResponse;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

 
namespace Coordination
{
 
//     struct EtcdKeeperRequest;
    struct EtcdKeeperRequest;
    using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
 
 
/** Looks like ZooKeeper but stores all data in memory of server process.
  * All data is not shared between different servers and is lost after server restart.
  *
  * The only purpose is to more simple testing for interaction with ZooKeeper within a single server.
  * This still makes sense, because multiple replicas of a single table can be created on a single server,
  *  and it is used to test replication logic.
  *
  * Does not support ACLs. Does not support NULL node values.
  *
  * NOTE: You can add various failure modes for better testing.
  */
    class EtcdKeeper : public IKeeper
    {
    public:
        using XID = int32_t;

        EtcdKeeper(const String & root_path_, Poco::Timespan operation_timeout_);
        ~EtcdKeeper() override;
 
        bool isExpired() const override { return expired; }
        int64_t getSessionID() const override { return 0; }
 
 
        void create(
                const String & path,
                const String & data,
                bool is_ephemeral,
                bool is_sequential,
                const ACLs & acls,
                CreateCallback callback) override;
 
        void remove(
                const String & path,
                int32_t version,
                RemoveCallback callback) override;
 
        void exists(
                const String & path,
                ExistsCallback callback,
                WatchCallback watch) override;
 
        void get(
                const String & path,
                GetCallback callback,
                WatchCallback watch) override;
 
        void set(
                const String & path,
                const String & data,
                int32_t version,
                SetCallback callback) override;
 
        void list(
                const String & path,
                ListCallback callback,
                WatchCallback watch) override;
 
        void check(
                const String & path,
                int32_t version,
                CheckCallback callback) override;
 
        void multi(
                const Requests & requests,
                MultiCallback callback) override;
 
 
        struct EtcdNode
        {
            String data;
            ACLs acls;
            bool is_ephemeral = false;
            bool is_sequental = false;
            Stat stat{};
            int32_t seq_num = 0;
            String unpursed_data;
            void serialize();
            void deserialize();
        };

        struct Call { 
            Call() = default;
            Call(const Call &) = default;
            Call & operator=(const Call &) = default;
            virtual ~Call() = default;
        };

        struct AsyncCall : virtual Call { 
            Status status;
            XID xid;
            // RequestInfo request_info;            
        };

        struct AsyncPutCall final : AsyncCall 
        {
            AsyncPutCall() {}
            AsyncPutCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            PutResponse response;
            std::unique_ptr<ClientAsyncResponseReader<PutResponse>> response_reader;
        };

        struct AsyncDeleteRangeCall final : AsyncCall 
        {
            AsyncDeleteRangeCall() {}
            AsyncDeleteRangeCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            DeleteRangeResponse response;
            std::unique_ptr<ClientAsyncResponseReader<DeleteRangeResponse>> response_reader;
        };

        struct AsyncRangeCall final : AsyncCall 
        {
            AsyncRangeCall() {}
            AsyncRangeCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            RangeResponse response;
            std::unique_ptr<ClientAsyncResponseReader<RangeResponse>> response_reader;
        };

        struct AsyncTxnCall final : AsyncCall 
        {
            AsyncTxnCall() {}
            AsyncTxnCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            TxnResponse response;
            std::unique_ptr<ClientAsyncResponseReader<TxnResponse>> response_reader;
        };

        // struct AsyncWatchCall final : AsyncCall 
        // {
        //     AsyncWatchCall() {}
        //     AsyncWatchCall(const AsyncCall & base) : AsyncCall(base) {}
        //     ClientContext context;
        //     etcdserverpb::WatchResponse response;
        //     std::unique_ptr<ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> response_reader;
        // };
  
        using WatchCallbacks = std::vector<WatchCallback>;
        using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

        std::unique_ptr<KV::Stub> kv_stub_;
        CompletionQueue kv_cq_;

        std::unique_ptr<Watch::Stub> watch_stub_;
        CompletionQueue watch_cq_;

        std::unique_ptr<KV::Stub> lease_stub_;
        CompletionQueue lease_cq_;

        void call_watch_request(
            const std::string & key,
            bool list_watch,
            std::unique_ptr<Watch::Stub> & stub_,
            CompletionQueue & cq_);

        void read_watch_response();
 
    private:

        std::atomic<XID> next_xid {1};

        using clock = std::chrono::steady_clock;
 
        struct RequestInfo
        {
            EtcdKeeperRequestPtr request;
            ResponseCallback callback;
            WatchCallback watch;
            clock::time_point time;
        };

        String root_path;
        ACLs default_acls;
 
        Poco::Timespan operation_timeout;
 
        std::mutex push_request_mutex;
        std::atomic<bool> expired{false};
 
        int64_t zxid = 0;
 
        Watches watches;
        Watches list_watches;   /// Watches for 'list' request (watches on children).
 
        void createWatchCallBack(const String & path);
 
        using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
        RequestsQueue requests_queue{1};

        using Operations = std::map<XID, RequestInfo>;

        Operations operations;
        std::mutex operations_mutex;
 
        void pushRequest(RequestInfo && request);
 
        void finalize();
 
        ThreadFromGlobalPool call_thread;
 
        void callThread();

        ThreadFromGlobalPool complete_thread;

        void completeThread();

        ThreadFromGlobalPool watch_complete_thread;

        void watchCompleteThread();

        // tmp
        std::unique_ptr<ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> stream_;
        ClientContext context_;
        etcdserverpb::WatchResponse response_;

        std::unique_ptr<PutRequest> prepare_put_request(const std::string &, const std::string &);
        std::unique_ptr<RangeRequest> prepare_range_request(const std::string &);
        std::unique_ptr<DeleteRangeRequest> prepare_delete_range_request(const std::string &);
        std::unique_ptr<TxnRequest> prepare_txn_request(
            const std::string &,
            Compare::CompareTarget ,
            Compare::CompareResult ,
            Int64 ,
            Int64 ,
            Int64 );

        void call_put_request(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<PutRequest> );

        void call_range_request(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<RangeRequest> );

        void call_delete_range_request(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<DeleteRangeRequest> );
        
    };

    struct EtcdKeeperResponse;
    using EtcdKeeperResponsePtr = std::shared_ptr<EtcdKeeperResponse>;
 
}