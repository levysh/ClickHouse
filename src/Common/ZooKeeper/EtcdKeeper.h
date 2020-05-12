#pragma once

#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>
#include <common/logger_useful.h>

#include <Poco/Timespan.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/StringUtils/StringUtils.h>

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
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

namespace Coordination
{
    struct EtcdKey;
    struct EtcdKeeperRequest;
    using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>;

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
    };

    struct Call
    {
        Call() = default;
        Call(const Call &) = default;
        Call & operator=(const Call &) = default;
        virtual ~Call() = default;
    };

    struct AsyncCall : virtual Call
    {
        Status status;
        XID xid;
        int responses;
    };

    struct AsyncTxnCall final : AsyncCall
    {
        AsyncTxnCall() {}
        AsyncTxnCall(const AsyncCall & base) : AsyncCall(base) {}
        ClientContext context;
        TxnResponse response;
        std::unique_ptr<ClientAsyncResponseReader<TxnResponse>> response_reader;
    };
    struct TxnRequests;

    using WatchCallbacks = std::vector<WatchCallback>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;
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
        Watches list_watches;
        std::mutex watches_mutex;

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

        std::unique_ptr<ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> stream;
        ClientContext context;
        etcdserverpb::WatchResponse watch_response;

        std::unique_ptr<KV::Stub> kv_stub;
        CompletionQueue kv_cq;

        std::unique_ptr<Watch::Stub> watch_stub;
        CompletionQueue watch_cq;

        std::unique_ptr<KV::Stub> lease_stub;
        CompletionQueue lease_cq;

        void callWatchRequest(
            const std::string & key,
            bool list_watch,
            std::unique_ptr<Watch::Stub> & stub,
            CompletionQueue & cq);

        void readWatchResponse();

        std::unique_ptr<PutRequest> preparePutRequest(const String &, const String &);
        std::unique_ptr<RangeRequest> prepareRangeRequest(const String &);
        std::unique_ptr<DeleteRangeRequest> prepareDeleteRangeRequest(const String &);
    };

    struct EtcdKeeperResponse;
    using EtcdKeeperResponsePtr = std::shared_ptr<EtcdKeeperResponse>;
    using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
    using EtcdKeeperRequests = std::vector<EtcdKeeperRequestPtr>;

}
