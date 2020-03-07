#include <boost/algorithm/string.hpp>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
 
#include <sstream>
#include <iomanip>
#include <iostream>


namespace Coordination
{
    enum class WatchConnType {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };

    static String parentPath(const String & path)
    {
        auto rslash_pos = path.rfind('/');
        if (rslash_pos > 0)
            return path.substr(0, rslash_pos);
        return "/";
    }

    static String baseName(const String & path)
    {
        auto rslash_pos = path.rfind('/');
        return path.substr(rslash_pos + 1);
    }

    void EtcdKeeper::EtcdNode::serialize() {
        std::vector<std::string> fields;
        fields.push_back(data);
        fields.push_back(is_ephemeral ? "1" : "0");
        fields.push_back(is_sequental ? "1" : "0");
        for (auto arg : stat.as_vector()) {
            fields.push_back(std::to_string(arg));
        }
        fields.push_back(std::to_string(seq_num));
        unpursed_data = "";
        for (auto field : fields) {
            unpursed_data += field + ';';
        }
    }

    void EtcdKeeper::EtcdNode::deserialize() {
        std::vector<std::string> fields;
        boost::split(fields, unpursed_data, boost::is_any_of(";"));
        data = fields[0];
        is_ephemeral = fields[1] == "1" ? true :false;
        is_sequental = fields[2] == "1" ? true :false;
        std::vector<std::string> stat_vector(fields.begin() + 2, fields.begin() + 13);
        seq_num = std::stoi(fields[14]);
    }

    std::unique_ptr<PutRequest> prepare_put_request(const std::string & key, const std::string & value)
    {
        std::unique_ptr<PutRequest> request = std::make_unique<PutRequest>();
        request->set_key(key);
        request->set_value(value);
        request->set_prev_kv(true);
        return request;
    }
        
    std::unique_ptr<RangeRequest> prepare_range_request(const std::string & key)
    {
        std::unique_ptr<RangeRequest> request = std::make_unique<RangeRequest>();
        request->set_key(key);
        return request;
    }
        
        
    std::unique_ptr<DeleteRangeRequest> prepare_delete_range_request(const std::string & key)
    {
        std::unique_ptr<DeleteRangeRequest> request = std::make_unique<DeleteRangeRequest>();
        request->set_key(key);
        // request.set_range_end(path);
        request->set_prev_kv(true);
        return request;
    }
        
        
    TxnRequest prepare_txn_request(
        const std::string & key,
        Compare::CompareTarget target,
        Compare::CompareResult result,
        Int64 version,
        Int64 create_revision,
        Int64 mod_revision
        // std::string & value = nullptr)
    )
    {
        TxnRequest txn_request;
        Compare* compare = txn_request.add_compare();
        compare->set_key(key);
        compare->set_target(target);
        compare->set_result(result);
        if (target == Compare::CompareTarget::Compare_CompareTarget_VERSION) {
            compare->set_version(version);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_CREATE) {
            compare->set_create_revision(create_revision);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_MOD) {
            compare->set_mod_revision(mod_revision);
        }
        // if (value) {
        //     compare->set_value(value);
        // }
        return txn_request;
    }

    void call_put_request(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<PutRequest> request) 
    {
        EtcdKeeper::AsyncPutCall* call = new EtcdKeeper::AsyncPutCall(call_);
        std::cout << "ADDRRRRRPUT  " << (void*)call << "\n";
        call->response_reader =
            stub_->PrepareAsyncPut(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void call_range_request(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<RangeRequest> request) 
    {
        EtcdKeeper::AsyncRangeCall* call = new EtcdKeeper::AsyncRangeCall(call_);
        std::cout << "ADDRRRRRANGE  " << (void*)call << "\n";
        call->response_reader =
            stub_->PrepareAsyncRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void call_delete_range_request(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<DeleteRangeRequest> request)
    {
        EtcdKeeper::AsyncDeleteRangeCall* call = new EtcdKeeper::AsyncDeleteRangeCall(call_);
        std::cout << "ADDRRRRREMOVE  " << (void*)call << "\n";
        call->response_reader =
            stub_->PrepareAsyncDeleteRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void call_txn_request(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<TxnRequest> request)
    {
        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader =
            stub_->PrepareAsyncTxn(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void EtcdKeeper::call_watch_request(
        const std::string & key,
        bool list_watch,
        std::unique_ptr<Watch::Stub> & stub_,
        CompletionQueue & cq_)
    {
        etcdserverpb::WatchRequest request;
        etcdserverpb::WatchResponse response;
        etcdserverpb::WatchCreateRequest create_request;
        create_request.set_key(key);
        request.mutable_create_request()->CopyFrom(create_request);
        // EtcdKeeper::AsyncWatchCall* call = new EtcdKeeper::AsyncWatchCall();
        // std::cout << "ADDRWATCH  " << (void*)call << "\n";
        // auto stream = stub_->AsyncWatch(&call->context, &cq_, (void*)"create");
        // stream->Write(request, (void*)"write");
        // stream->Read(&response, (void*)this);
        // call->response_reader =
        //     stub_->AsyncWatch(&call->context, &cq_, (void*)"create");
        stream_ = stub_->AsyncWatch(&context_, &cq_, (void*)WatchConnType::CONNECT);
        // call->response_reader->StartCall((void*)call);
        std::cout << "WATCHWRITE" << "\n";
        stream_->Write(request, (void*)WatchConnType::WRITE);
        // std::cout << "WATCHREED" << "\n";
        // stream_->Read(&response_, (void*)WatchConnType::READ);
        // call->response_reader->WritesDone((void*)call);
        // call->response_reader->Finish(&call->status, (void*)call);
        // call->response_reader->WritesDone((void*)"writes done");
        std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WATCH " << key << list_watch << std::endl;
    }

    void EtcdKeeper::read_watch_response()
    {
        stream_->Read(&response_, (void*)WatchConnType::READ);
        if (response_.created()) {
            std::cout << "ANSWERCREATED" << std::endl;
            return;
        }
        if (response_.events_size()) {
            for (auto response : response_.events()) {
                std::cout << "ANSWER       " << response.kv().key() << std::endl;
            }
        }
    }
 
    struct EtcdKeeperRequest : virtual Request
    {
        EtcdKeeper::XID xid = 0;
        virtual bool isMutable() const { return false; }
        virtual EtcdKeeperResponsePtr makeResponse() const = 0;
        virtual void call(
            EtcdKeeper::AsyncCall& call,
            std::unique_ptr<KV::Stub>& kv_stub_,
            CompletionQueue& kv_cq_) const = 0;
        // virtual void processWatches(EtcdKeeper::Watches & /*watches*/, EtcdKeeper::Watches & /*list_watches*/) const {}
    };

    struct EtcdKeeperResponse : virtual Response {
        virtual ~EtcdKeeperResponse() {}
        virtual void readImpl(void* got_tag) = 0;
    };
 
 
    // static void processWatchesImpl(const String & path, EtcdKeeper::Watches & watches, EtcdKeeper::Watches & list_watches)
    // {
    //     WatchResponse watch_response;
    //     watch_response.path = path;
 
    //     auto it = watches.find(watch_response.path);
    //     if (it != watches.end())
    //     {
    //         for (auto & callback : it->second)
    //             if (callback)
    //                 callback(watch_response);
 
    //         watches.erase(it);
    //     }
 
    //     WatchResponse watch_list_response;
    //     watch_list_response.path = parentPath(path);
 
    //     it = list_watches.find(watch_list_response.path);
    //     if (it != list_watches.end())
    //     {
    //         for (auto & callback : it->second)
    //             if (callback)
    //                 callback(watch_list_response);
 
    //         list_watches.erase(it);
    //     }
    // }
 
 
    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        EtcdKeeperCreateRequest() {}
        EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            EtcdKeeper::EtcdNode test_node;
            test_node.data = data;
            // test_node.acls = default_acls;
            // test_node.is_ephemeral = true;
            // test_node.stat = Stat();
            // test_node.seq_num = 13;

            test_node.serialize();

            call_put_request(_call, kv_stub_, kv_cq_, prepare_put_request(path, test_node.unpursed_data));
        }
 
        // void processWatches(EtcdKeeper::Watches & node_watches, EtcdKeeper::Watches & list_watches) const override
        // {
        //     processWatchesImpl(getPath(), node_watches, list_watches);
        // }
    };

    struct EtcdKeeperCreateResponse final : CreateResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncPutCall* call = static_cast<EtcdKeeper::AsyncPutCall*>(got_tag);
            error = Error::ZOK;
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CREATE Greeter received: " << call->response.prev_kv().key() << std::endl;
        }
    };
 
    struct EtcdKeeperRemoveRequest final : RemoveRequest, EtcdKeeperRequest
    {
        EtcdKeeperRemoveRequest() {}
        EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            call_delete_range_request(_call, kv_stub_, kv_cq_, prepare_delete_range_request(path));
        }
 
        // void processWatches(EtcdKeeper::Watches & node_watches, EtcdKeeper::Watches & list_watches) const override
        // {
        //     processWatchesImpl(getPath(), node_watches, list_watches);
        // }
    };

    struct EtcdKeeperRemoveResponse final : RemoveResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncDeleteRangeCall* create_call = static_cast<EtcdKeeper::AsyncDeleteRangeCall*>(got_tag);
            if (create_call->response.deleted()) {
                error = Error::ZOK;
            } else {
                error = Error::ZNONODE;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%REMOVE Greeter receivedddd: " << create_call->response.deleted() << std::endl;        
        }
    };
 
    struct EtcdKeeperExistsRequest final : ExistsRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            std::string data = "";
            // call_put_request(_call, kv_stub_, kv_cq_, prepare_put_request(path, data));
            call_range_request(_call, kv_stub_, kv_cq_, prepare_range_request(path));
        }
        // ResponsePtr createResponse() const { return std::make_shared<RemoveResponse>(); }
    };

    struct EtcdKeeperExistsResponse final : ExistsResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncRangeCall* call = static_cast<EtcdKeeper::AsyncRangeCall*>(got_tag);

            if (call->response.count()) {
                error = Error::ZOK;
            } else {
                error = Error::ZNONODE;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%EXISTS Greeter received: " << call->response.count() << std::endl;
        }
    };
 
    struct EtcdKeeperGetRequest final : GetRequest, EtcdKeeperRequest
    {
        EtcdKeeperGetRequest() {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            call_range_request(_call, kv_stub_, kv_cq_, prepare_range_request(path));
        }
    };

    struct EtcdKeeperGetResponse final : GetResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncRangeCall* call = static_cast<EtcdKeeper::AsyncRangeCall*>(got_tag);
            if (call->response.count()) {
                for (auto kv : call->response.kvs()) {
                    data = kv.value();
                }
                stat = Stat();
                data = "";
                error = Error::ZOK;
            } else {
                error = Error::ZNONODE;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%GET Greeter received: " << call->response.count() << std::endl;
        }
    };
 
    struct EtcdKeeperSetRequest final : SetRequest, EtcdKeeperRequest
    {
        EtcdKeeperSetRequest() {}
        EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
            if (version == -1) {
                compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            }
            TxnRequest txn_request = prepare_txn_request(
                parentPath(path + "/"),
                Compare::CompareTarget::Compare_CompareTarget_VERSION,
                compare_result,
                version,
                0,
                0);
            
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(prepare_range_request(path).release());

            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_put(prepare_put_request(path, data).release());

            call_txn_request(_call, kv_stub_, kv_cq_, std::make_unique<TxnRequest>(txn_request));
        }
 
        // void processWatches(EtcdKeeper::Watches & node_watches, EtcdKeeper::Watches & list_watches) const override
        // {
        //     processWatchesImpl(getPath(), node_watches, list_watches);
        // }
    };

    struct EtcdKeeperSetResponse final : SetResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            if (call->response.succeeded()) {
                error = Error::ZOK;
            }
            else if (call->response.responses()[0].response_range().count())
            {
                error = Error::ZNONODE;
            }
            else 
            {
                error = Error::ZBADVERSION;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%SET Greeter received: " << std::endl;
        }
    };
 
    struct EtcdKeeperListRequest final : ListRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            TxnRequest txn_request = prepare_txn_request(
                parentPath(path + "/"),
                Compare::CompareTarget::Compare_CompareTarget_VERSION,
                Compare::CompareResult::Compare_CompareResult_NOT_EQUAL,
                -1,
                0,
                0);

            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_range(prepare_range_request(path).release());
            call_txn_request(_call, kv_stub_, kv_cq_, std::make_unique<TxnRequest>(txn_request));
        }
    };

    struct EtcdKeeperListResponse final : ListResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            if (!call->response.succeeded()) {
                error = Error::ZNONODE;
            } else {
                for (auto resp : call->response.responses()) {
                    for (auto kv : resp.response_range().kvs()) {
                        names.emplace_back(baseName(kv.key()));
                    }
                }
                error = Error::ZOK;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%List Greeter received: " << call->response.responses()[0].response_range().count() << std::endl;
        }
    };
 
    struct EtcdKeeperCheckRequest final : CheckRequest, EtcdKeeperRequest
    {
        EtcdKeeperCheckRequest() {}
        EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void call(EtcdKeeper::AsyncCall& _call, std::unique_ptr<KV::Stub>& kv_stub_, CompletionQueue& kv_cq_) const override {
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
            if (version == -1) {
                compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            }
            TxnRequest txn_request = prepare_txn_request(
                parentPath(path + "/"),
                Compare::CompareTarget::Compare_CompareTarget_VERSION,
                compare_result,
                version,
                0,
                0);
            
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(prepare_range_request(path).release());
            call_txn_request(_call, kv_stub_, kv_cq_, std::make_unique<TxnRequest>(txn_request));
        }
    };

    struct EtcdKeeperCheckResponse final : CheckResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            if (call->response.succeeded()) {
                error = Error::ZOK;
            }
            else if (call->response.responses()[0].response_range().count())
            {
                error = Error::ZNONODE;
            }
            else 
            {
                error = Error::ZBADVERSION;
            }
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CHECK Greeter received: " << call->response.succeeded() << std::endl;
        }
    };
 
    struct EtcdKeeperMultiRequest final : MultiRequest, EtcdKeeperRequest
    {
        EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            requests.reserve(generic_requests.size());
 
            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    auto create = std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create);
                    requests.push_back(create);
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    requests.push_back(std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove));
                }
                else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    requests.push_back(std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set));
                }
                else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    requests.push_back(std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check));
                }
                else
                    throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
            }
        }

        void call(EtcdKeeper::AsyncCall& , std::unique_ptr<KV::Stub>& , CompletionQueue& ) const override {
        }
 
        // void processWatches(EtcdKeeper::Watches & node_watches, EtcdKeeper::Watches & list_watches) const override
        // {
        //     for (const auto & generic_request : requests)
        //         dynamic_cast<const EtcdKeeperRequest &>(*generic_request).processWatches(node_watches, list_watches);
        // }
 
        EtcdKeeperResponsePtr makeResponse() const override;
    };

    struct EtcdKeeperMultiResponse final : MultiResponse, EtcdKeeperResponse
    {
        void readImpl(void* got_tag) override
        {
            EtcdKeeper::AsyncPutCall* create_call = static_cast<EtcdKeeper::AsyncPutCall*>(got_tag);
            std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%MULTI Greeter received: " << create_call->response.prev_kv().key() << std::endl;
        }
    };
  
    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponse() const { return std::make_shared<EtcdKeeperCreateResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponse() const { return std::make_shared<EtcdKeeperRemoveResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponse() const { return std::make_shared<EtcdKeeperExistsResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponse() const { return std::make_shared<EtcdKeeperGetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponse() const { return std::make_shared<EtcdKeeperSetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponse() const { return std::make_shared<EtcdKeeperListResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponse() const { return std::make_shared<EtcdKeeperCheckResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponse() const { return std::make_shared<EtcdKeeperMultiResponse>(); }
 
 
    EtcdKeeper::EtcdKeeper(const String & root_path_, Poco::Timespan operation_timeout_)
            : root_path(root_path_), operation_timeout(operation_timeout_)
    {

        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "INIT" << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";


        std::string stripped_address = "localhost:2379";
        std::shared_ptr<Channel> channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        kv_stub_= KV::NewStub(channel);

        std::shared_ptr<Channel> watch_channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        watch_stub_= Watch::NewStub(watch_channel);
 
        if (!root_path.empty())
        {
            if (root_path.back() == '/')
                root_path.pop_back();
        }
 
        call_thread = ThreadFromGlobalPool([this] { callThread(); });
        complete_thread = ThreadFromGlobalPool([this] { completeThread(); });
        watch_complete_thread = ThreadFromGlobalPool([this] { watchCompleteThread(); });
    }
 
 
    EtcdKeeper::~EtcdKeeper()
    {

        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "DESTR" << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";

        try
        {
            finalize();
            if (call_thread.joinable())
                call_thread.join();
            if (complete_thread.joinable())
                complete_thread.join();
            if (watch_complete_thread.joinable())
                watch_complete_thread.join();

        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void EtcdKeeper::callThread()
    {
        setThreadName("EtcdKeeperCall");
 
        try
        {
            while (!expired)
            {
                RequestInfo info;
 
                UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());
                if (requests_queue.tryPop(info, max_wait))
                {
                    if (expired)
                        break;

                    if (info.watch)
                    {
                        std::cout << "WAAAAAAAATTAATTAAATTACH" << std::endl;
                        bool list_watch = false;
                        if (dynamic_cast<const ListRequest *>(info.request.get())) {
                            list_watch = true;
                        }
                        call_watch_request(info.request->getPath(), list_watch, watch_stub_, watch_cq_);
                    }

                    std::lock_guard lock(operations_mutex);
                    operations[info.request->xid] = info;

                    // for(auto it = operations.cbegin(); it != operations.cend(); ++it)
                    // {
                    //     std::cout << "oppppppppppp" << it->first << "\n";
                    // }
 
                    // if (info.watch)
                    // {
                    //     auto & watches_type = dynamic_cast<const ListRequest *>(info.request.get())
                    //                           ? list_watches
                    //                           : watches;
 
                    //     watches_type[info.request->getPath()].emplace_back(std::move(info.watch));
                    // }

                    if (expired)
                        break;

                    info.request->addRootPath(root_path);

                    EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                    call->xid = info.request->xid;

                    std::cout << "ADDR  " << (void*)call << "\n";

                    info.request->call(*call, kv_stub_, kv_cq_);
 
                    // ++zxid;
 
                    // info.request->addRootPath(root_path);
                    // ResponsePtr response = info.request->process(container, zxid);
                    // if (response->error == Error::ZOK)
                    //     info.request->processWatches(watches, list_watches);
 
                    // response->removeRootPath(root_path);
                    // if (info.callback)
                    //     info.callback(*response);
                    delete call;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
    }

    void EtcdKeeper::completeThread() {
        setThreadName("EtcdKeeperComplete");

        try
        {
            RequestInfo request_info;
            EtcdKeeperResponsePtr response;

            void* got_tag;
            bool ok = false;

            while (kv_cq_.Next(&got_tag, &ok)) {
                std::cout << "BAD_CAST  " << got_tag << "\n";
                GPR_ASSERT(ok);
                for(auto it = operations.cbegin(); it != operations.cend(); ++it)
                {
                    std::cout << "oppppppppppp" << it->first << "\n";
                }
                if (got_tag)
                {
                    auto call = static_cast<AsyncCall*>(got_tag);

                    // auto tmp_call = static_cast<AsyncPutCall*>(got_tag)
                    // if (auto tmp_call = static_cast<AsyncPutCall*>(got_tag)) {
                    //     call = std::make_shared<AsyncPutCall>(*tmp_call);
                    //     // auto call = static_cast<AsyncCall*>(got_tag);

                    // } else if (auto tmp_call = static_cast<AsyncRangeCall*>(got_tag)) {
                    //     call = std::make_shared<AsyncRangeCall>(*tmp_call);
                    // }

                    XID xid = call->xid;
                    std::cout << "???????????????????????????????????????????????????????XID  " << xid << "\n";

                    // for(auto it = operations.cbegin(); it != operations.cend(); ++it)
                    // {
                    //     std::cout << "oppppppppppp" << it->first << "\n";
                    // }

                    auto it = operations.find(xid);
                    if (it == operations.end())
                        throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

                    /// After this point, we must invoke callback, that we've grabbed from 'operations'.
                    /// Invariant: all callbacks are invoked either in case of success or in case of error.
                    /// (all callbacks in 'operations' are guaranteed to be invoked)

                    request_info = std::move(it->second);
                    operations.erase(it);

                    response = request_info.request->makeResponse();
                    // if (!response)
                    //     response = request_info.request->makeResponse();

                    if (!call->status.ok())
                    {
                        std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%RPC failed" << call->status.error_message() << std::endl;
                    }
                    else
                    {
                        std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%REEEDDD" << call->status.error_message() << std::endl;

                        response->readImpl(got_tag);
                        response->removeRootPath(root_path);
                    }
                    
                    if (request_info.callback)
                        request_info.callback(*response);
                } 
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
        
    }

    void EtcdKeeper::watchCompleteThread() {
        setThreadName("EtcdKeeperWatchComplete");

        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! COMPLETE" << "\n";

        void* got_tag;
        bool ok = false;

        try
        {
            while (watch_cq_.Next(&got_tag, &ok))
            {
                // test

                if (ok) {
                    switch (static_cast<WatchConnType>(reinterpret_cast<long>(got_tag))) {
                        case WatchConnType::READ:
                            std::cout << "Read a new message." << std::endl;
                        case WatchConnType::WRITE:
                            std::cout << "Sending message (async)." << std::endl;
                            read_watch_response();
                            // AsyncHelloRequestNextMessage();
                        case WatchConnType::CONNECT:
                            std::cout << "Server connected." << std::endl;
                        case WatchConnType::WRITES_DONE:
                            std::cout << "Server disconnecting." << std::endl;
                        // case WatchConnType::FINISH:
                        //     std::cout << "Client finish; status = "
                        //             << (finish_status_.ok() ? "ok" : "cancelled")
                        //             << std::endl;
                        //     context_.TryCancel();
                        //     cq_.Shutdown();
                        //     break;
                        // default:
                        //     std::cerr << "Unexpected tag " << got_tag << std::endl;
                        //     GPR_ASSERT(false);
                    }
                }

                // test


                // std::cout << "ADDR  WATCH" << got_tag << "\n";
                // if(ok == false || (got_tag == (void*)WatchConnType::WRITES_DONE))
                // {
                //     std::cout << "BREAK" << "\n";
                //     break;
                // }
                // if(got_tag == (void*)this)
                // {
                //     std::cout << "SIZE    " << response_.events_size() << "\n";
                //     if(response_.events_size())
                //     {
                //         std::cout << "0WRITESDONE" << "\n";
                //         stream_->WritesDone((void*)WatchConnType::WRITES_DONE);
                //         std::cout << "1WRITESDONE" << "\n";
                //     }
                //     else
                //     {
                //         std::cout << "0READ" << "\n";
                //         stream_->Read(&response_, (void*)WatchConnType::READ);
                //         std::cout << "1READ" << "\n";
                //     } 
                // }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
        
    }
 
    void EtcdKeeper::finalize()
    {
        std::cout << "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%FINALIZE" << std::endl;

        {
            std::lock_guard lock(push_request_mutex);
 
            if (expired)
                return;
            expired = true;
        }
 
        call_thread.join();
        complete_thread.join();
        watch_complete_thread.join();
 
        try
        {
        //     {
        //         for (auto & path_watch : watches)
        //         {
        //             WatchResponse response;
        //             response.type = SESSION;
        //             response.state = EXPIRED_SESSION;
        //             response.error = ZSESSIONEXPIRED;
 
        //             for (auto & callback : path_watch.second)
        //             {
        //                 if (callback)
        //                 {
        //                     try
        //                     {
        //                         callback(response);
        //                     }
        //                     catch (...)
        //                     {
        //                         tryLogCurrentException(__PRETTY_FUNCTION__);
        //                     }
        //                 }
        //             }
        //         }
 
        //         watches.clear();
        //     }
 
            RequestInfo info;
            while (requests_queue.tryPop(info))
            {
                if (info.callback)
                {
                    // ResponsePtr response = info.request->createResponse();
                    // response->error = ZSESSIONEXPIRED;
                    // try
                    // {
                    //     info.callback(*response);
                    // }
                    // catch (...)
                    // {
                    //     tryLogCurrentException(__PRETTY_FUNCTION__);
                    // }
                }
                if (info.watch)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = ZSESSIONEXPIRED;
                    try
                    {
                        info.watch(response);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
 
    void EtcdKeeper::pushRequest(RequestInfo && info)
    {
        try
        {
            info.time = clock::now();
 
            if (!info.request->xid)
            {
                info.request->xid = next_xid.fetch_add(1);
                std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "XID" << next_xid << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
                if (info.request->xid < 0)
                    throw Exception("XID overflow", ZSESSIONEXPIRED);
            }
            /// We must serialize 'pushRequest' and 'finalize' (from processingThread) calls
            ///  to avoid forgotten operations in the queue when session is expired.
            /// Invariant: when expired, no new operations will be pushed to the queue in 'pushRequest'
            ///  and the queue will be drained in 'finalize'.
            std::lock_guard lock(push_request_mutex);
 
            if (expired)
                throw Exception("Session expired", ZSESSIONEXPIRED);
 
            if (!requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
                throw Exception("Cannot push request to queue within operation timeout", ZOPERATIONTIMEOUT);
        }
        catch (...)
        {
            finalize();
            throw;
        }
    }
 
 
    void EtcdKeeper::create(
            const String & path,
            const String & data,
            bool is_ephemeral,
            bool is_sequential,
            const ACLs &,
            CreateCallback callback)
    {
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "CREATE" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperCreateRequest request;
        request.path = path;
        request.data = data;
        request.is_ephemeral = is_ephemeral;
        request.is_sequential = is_sequential;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperCreateRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::remove(
            const String & path,
            int32_t version,
            RemoveCallback callback)
    {
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "REMOVE" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperRemoveRequest request;
        request.path = path;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperRemoveRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::exists(
            const String & path,
            ExistsCallback callback,
            WatchCallback watch)
    {
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "EXISTS" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperExistsRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperExistsRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::get(
            const String & path,
            GetCallback callback,
            WatchCallback watch)
    { 
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "GET" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperGetRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperGetRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::set(
            const String & path,
            const String & data,
            int32_t version,
            SetCallback callback)
    { 
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "SET" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperSetRequest request;
        request.path = path;
        request.data = data;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperSetRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::list(
            const String & path,
            ListCallback callback,
            WatchCallback watch)
    { 
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "LIST" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperListRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperListRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::check(
            const String & path,
            int32_t version,
            CheckCallback callback)
    { 
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "CHECK" << path << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperCheckRequest request;
        request.path = path;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperCheckRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::multi(
            const Requests & requests,
            MultiCallback callback)
    { 
        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << "MULTI" << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
        EtcdKeeperMultiRequest request(requests);
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperMultiRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
}
