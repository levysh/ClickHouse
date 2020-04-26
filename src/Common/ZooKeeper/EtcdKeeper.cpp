#include <boost/algorithm/string.hpp>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <common/logger_useful.h>
 
#include <sstream>
#include <iomanip>
#include <iostream>
#include <unordered_map>


namespace Coordination
{
    std::unordered_map<std::string, int32_t> seqs;

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

    PutRequest preparePutRequest(const std::string & key, const std::string & value)
    {
        PutRequest request = PutRequest();

        request.set_key(key);
        request.set_value(value);
        request.set_prev_kv(true);
        return request;
    }
        
    RangeRequest prepareRangeRequest(const std::string & key, bool with_prefix=false)
    {
        RangeRequest request = RangeRequest();
        request.set_key(key);
        std::string range_end(key);
        if(with_prefix)
        {
            std::cout << "WITH PREFIX" << std::endl;
            int ascii = (int)range_end[range_end.length() - 1];
            range_end.back() = ascii+1;
            request.set_range_end(range_end);
        }
        return request;
    }
        
        
    DeleteRangeRequest prepareDeleteRangeRequest(const std::string & key)
    {
        DeleteRangeRequest request = DeleteRangeRequest();
        request.set_key(key);
        request.set_prev_kv(true);
        return request;
    }
        
    Compare prepareCompare(
        const std::string & key,
        Compare::CompareTarget target,
        Compare::CompareResult result,
        Int64 version,
        Int64 create_revision,
        Int64 mod_revision
    )
    {
        Compare compare;
        compare.set_key(key);
        compare.set_target(target);
        compare.set_result(result);
        if (target == Compare::CompareTarget::Compare_CompareTarget_VERSION) {
            compare.set_version(version);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_CREATE) {
            compare.set_create_revision(create_revision);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_MOD) {
            compare.set_mod_revision(mod_revision);
        }
        return compare;
    }

    Compare prepareCompare(
        const std::string & key,
        std::string target,
        std::string result,
        Int64 value
    )
    {
        Compare compare;
        compare.set_key(key);
        Compare::CompareResult compare_result;
        if (result == "equal")
        {
            compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
        }
        compare.set_result(compare_result);
        Compare::CompareTarget compare_target;
        if (target == "version") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            compare.set_version(value);
        }
        if (target == "create") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_CREATE;
            compare.set_create_revision(value);
        }
        if (target == "mod") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_MOD;
            compare.set_mod_revision(value);
        }
        if (target == "value") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VALUE;
            compare.set_value(std::to_string(value));
        }
        compare.set_target(compare_target);
        return compare;
    }
        
    TxnRequest prepareTxnRequest(
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

    void callPutRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        PutRequest request,
        const std::string & path)
    {
        EtcdKeeper::AsyncPutCall* call = new EtcdKeeper::AsyncPutCall(call_);
        call->path = path;
        call->response_reader =
            stub_->PrepareAsyncPut(&call->context, request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callRangeRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<RangeRequest> request) 
    {
        EtcdKeeper::AsyncRangeCall* call = new EtcdKeeper::AsyncRangeCall(call_);
        call->response_reader =
            stub_->PrepareAsyncRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callDeleteRangeRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<DeleteRangeRequest> request)
    {
        EtcdKeeper::AsyncDeleteRangeCall* call = new EtcdKeeper::AsyncDeleteRangeCall(call_);
        call->response_reader =
            stub_->PrepareAsyncDeleteRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        EtcdKeeper::TxnRequests requests)
    {
        TxnRequest txn_request;
        for (auto txn_compare: requests.compares) {
            std::cout << "CMPR" << std::endl;
            Compare* compare = txn_request.add_compare();
            compare->CopyFrom(txn_compare);
        }
        RequestOp* req_success;
        for (auto success_range: requests.success_ranges)
        {
            std::cout << "RG" << success_range.key() << std::endl;
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_range(std::make_unique<RangeRequest>(success_range).release());
        }
        for (auto success_put: requests.success_puts)
        {
            std::cout << "CR" << success_put.key() << " " << success_put.value() << std::endl;
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_put(std::make_unique<PutRequest>(success_put).release());
        }
        for (auto success_delete_range: requests.success_delete_ranges)
        {
            std::cout << "DR" << success_delete_range.key() << std::endl;
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(success_delete_range).release());
        }
        for (auto failure_range: requests.failure_ranges)
        {
            std::cout << "FRG" << failure_range.key() << std::endl;
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(std::make_unique<RangeRequest>(failure_range).release());
        }
        for (auto failure_put: requests.failure_puts)
        {
            std::cout << "FCR" << failure_put.key() << std::endl;
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_put(std::make_unique<PutRequest>(failure_put).release());
        }
        for (auto failure_delete_range: requests.failure_delete_ranges)
        {
            std::cout << "FDR" << failure_delete_range.key() << std::endl;
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(failure_delete_range).release());
        }

        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub_->PrepareAsyncTxn(&call->context, txn_request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }


    void callTxnRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<TxnRequest> request)
    {
        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub_->PrepareAsyncTxn(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void EtcdKeeper::callWatchRequest(
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
        stream_->Write(request, (void*)WatchConnType::WRITE);
        LOG_DEBUG(log, "WATCH " << key << list_watch);
    }

    void EtcdKeeper::readWatchResponse()
    {
        stream_->Read(&response_, (void*)WatchConnType::READ);
        if (response_.created()) {
            LOG_ERROR(log, "Watch created");
        } else if (response_.events_size()) {
            std::cout << "WATCH RESP " << response_.DebugString() << std::endl;
            for (auto event : response_.events()) {
                String path = event.kv().key();
                WatchResponse watch_response;
                watch_response.path = path;

                auto it = watches.find(watch_response.path);
                if (it != watches.end())
                {
                    for (auto & callback : it->second)
                        if (callback)
                            callback(watch_response);

                    watches.erase(it);
                }

                WatchResponse watch_list_response;
                watch_list_response.path = parentPath(path);

                it = list_watches.find(watch_list_response.path);
                if (it != list_watches.end())
                {
                    for (auto & callback : it->second)
                        if (callback)
                            callback(watch_list_response);

                    list_watches.erase(it);
                }
            }
        } else {
            LOG_ERROR(log, "Returned watch without created flag and without event.");
        }
    }
 
    struct EtcdKeeperRequest : virtual Request
    {
        std::string process_path;
        EtcdKeeper::XID xid = 0;
        bool composite = false;
        EtcdKeeper::TxnRequests txn_requests;
        std::vector<ResponseOp> pre_call_responses;
        virtual bool isMutable() const { return false; }
        bool pre_call_called = false;
        bool post_call_called = false;
        void setPreCall()
        {
            pre_call_called = true;
        }
        virtual void call(EtcdKeeper::AsyncCall& call,
            std::unique_ptr<KV::Stub>& kv_stub_,
            CompletionQueue& kv_cq_) const
        {
            callRequest(call, kv_stub_, kv_cq_, txn_requests);
            setPreCall();
            txn_requests.clear();
        }
        virtual EtcdKeeperResponsePtr makeResponse() const = 0;
        virtual void preparePostCall() const = 0;
        virtual void preparePreCall() {}
        virtual EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) = 0;
        EtcdKeeperResponsePtr makeResponseFromRepeatedPtrField(bool compare_result, google::protobuf::RepeatedPtrField<ResponseOp> fields)
        {
            std::vector<ResponseOp> responses;
            for (auto field : fields) {
                responses.push_back(field);
            }
            return makeResponseFromResponses(compare_result, responses);
        }
        EtcdKeeperResponsePtr makeResponseFromTag(void* got_tag)
        {
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            return makeResponseFromRepeatedPtrField(call->response.succeeded(), call->response.responses());
        }
        bool callRequired(void* got_tag)
        {
            if (composite && !post_call_called) {
                EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
                for (auto field : call->response.responses()) {
                    pre_call_responses.push_back(field);
                }
                post_call_called = true;
                std::cout << "CALL REQ" << std::endl;
                return true;
            }
            else
            {
                std::cout << "CALL NOT REQ" << std::endl;
                return false;
            }
        }
        virtual void checkRequestForComposite() {}
        virtual void prepareCall() const {
            checkRequestForComposite();
            if (composite && !pre_call_called) {
                preparePreCall();
                return;
            }
            preparePostCall();
        }
    };

    using EtcdRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
    using EtcdRequests = std::vector<EtcdRequestPtr>;

    struct EtcdKeeperResponse : virtual Response {
        virtual ~EtcdKeeperResponse() {}
    };

    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        int32_t seq_num;
        EtcdKeeperCreateRequest() {}
        EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void setProcessPath(int32_t seq_num_)
        {
            std::cout << "SEQ NUM" << seq_num_ << std::endl;
            seq_num = seq_num_;
            if (is_sequential)
            {
                // int32_t seq_num = seqs[parentPath(path)]++;

                std::stringstream seq_num_str;
                seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

                process_path += seq_num_str.str();
            }
        }
        void preparePreCall()
        {
            txn_requests.success_ranges.push_back(prepareRangeRequest("/seq" + parentPath(path)));
        }
        void parsePreResponses()
        {
            process_path = path;
            for (auto resp : pre_call_responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        std::cout << "KEY" << kv.key() << std::endl;
                        if (kv.key() == "/seq" + parentPath(path))
                        {
                            setProcessPath(std::stoi(kv.value()));
                            break;
                        }
                    }
                }
            }
        }
        void preparePostCall() const
        {
            // TODO add creating metadata nodes
            parsePreResponses();
            std::cout << "CREATE " << process_path << std::endl;
            if (is_sequential)
            {
                txn_requests.compares.push_back(prepareCompare("/seq" + parentPath(process_path), "value", "equal", seq_num));
                txn_requests.success_puts.push_back(preparePutRequest("/seq" + parentPath(process_path), std::to_string(seq_num + 1)));
            }
            txn_requests.success_puts.push_back(preparePutRequest(process_path, data));
            txn_requests.success_puts.push_back(preparePutRequest("/seq" + process_path, std::to_string(0)));
        }
        void checkRequestForComposite() {
            if (is_sequential)
            {
                composite = true;
            }
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperCreateResponse final : CreateResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperRemoveRequest final : RemoveRequest, EtcdKeeperRequest
    {
        EtcdKeeperRemoveRequest() {}
        EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override {
            std::cout << "REMOVE " << path << std::endl;
            txn_requests.success_delete_ranges.emplace_back(prepareDeleteRangeRequest(path));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
    struct EtcdKeeperRemoveResponse final : RemoveResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperExistsRequest final : ExistsRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override
        {
            std::cout << "EXISTS " << path << std::endl;
            txn_requests.success_ranges.push_back(prepareRangeRequest(path));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperExistsResponse final : ExistsResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperGetRequest final : GetRequest, EtcdKeeperRequest
    {
        EtcdKeeperGetRequest() {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override
        {
            std::cout << "GET " << path << std::endl;
            txn_requests.success_ranges.push_back(prepareRangeRequest(path));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperGetResponse final : GetResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperSetRequest final : SetRequest, EtcdKeeperRequest
    {
        EtcdKeeperSetRequest() {}
        EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override
        {
            std::cout << "SET " << path << std::endl;
            // txn_requests.compares.push_back(prepareCompare(path, "version", version == -1 ? "no_equal" : "equal", version));
            // txn_requests.failure_ranges.push_back(prepareRangeRequest(path));
            txn_requests.success_puts.push_back(preparePutRequest(path, data));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperSetResponse final : SetResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperListRequest final : ListRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override {
            std::cout << "LIST " << path << std::endl;
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            Compare::CompareTarget compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            txn_requests.compares.push_back(prepareCompare(path, compare_target, compare_result, -1, 0, 0));
            txn_requests.success_ranges.push_back(prepareRangeRequest(path + "/", true));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperListResponse final : ListResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperCheckRequest final : CheckRequest, EtcdKeeperRequest
    {
        EtcdKeeperCheckRequest() {}
        EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void preparePostCall() const override
        {
            std::cout << "CHECK " << path << std::endl;
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
            if (version == -1) {
                compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            }
            Compare::CompareTarget compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            txn_requests.compares.push_back(prepareCompare(path, compare_target, compare_result, version, 0, 0));
            txn_requests.failure_ranges.push_back(prepareRangeRequest(path));
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperCheckResponse final : CheckResponse, EtcdKeeperResponse {};
 
using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
using EtcdKeeperRequests = std::vector<EtcdKeeperRequestPtr>;

    struct EtcdKeeperMultiRequest final : MultiRequest, EtcdKeeperRequest
    {
        EtcdKeeperRequests etcd_requests;

        EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            std::cout << "MULTI " << std::endl;
            etcd_requests.reserve(generic_requests.size());
            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    std::cout << "concrete_request_create" << std::endl;
                    // auto request = std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create);
                    // request->prepareCall();
                    // txn_requests += request->txn_requests;
                    // etcd_requests.push_back(std::move(request));
                    etcd_requests.push_back(std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create));
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    std::cout << "concrete_request_remove" << std::endl;
                    // auto request = std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove);
                    // request->prepareCall();
                    // txn_requests += request->txn_requests;
                    // etcd_requests.push_back(std::move(request));
                    etcd_requests.push_back(std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove));
                }
                else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    std::cout << "concrete_request_set" << std::endl;
                    // auto request = std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set);
                    // request->prepareCall();
                    // txn_requests += request->txn_requests;
                    // etcd_requests.push_back(std::move(request));
                    etcd_requests.push_back(std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set));
                }
                else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    std::cout << "concrete_request_check" << std::endl;
                    // auto request = std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check);
                    // request->prepareCall();
                    // txn_requests += request->txn_requests;
                    // etcd_requests.push_back(std::move(request));
                    etcd_requests.push_back(std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check));
                }
                else
                {
                    throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
                }
            }
        }

        void preparePreCall()
        {
            for (auto request : etcd_requests)
            {
                request->checkRequestForComposite();
                if (request->composite)
                {
                    std::cout << "COMPOSITE" << std::endl;
                    request->preparePreCall();
                    txn_requests += request->txn_requests;
                }
            }
        }

        void preparePostCall() const override {

            for (auto request : etcd_requests)
            {
                request->pre_call_responses = pre_call_responses;
                request->preparePostCall();
                txn_requests += request->txn_requests;
            }
            txn_requests.interaction();
        }

        void checkRequestForComposite() {
            std::cout << "MULTI COMP" << std::endl;
            if (!txn_requests.empty())
            {
                std::cout << "MULTI COMPOS" << std::endl;
                composite = true;
            }
        }

        void prepareCall() const {
            std::cout << "PREPARE" << std::endl;
            if (!pre_call_called)
            {
                preparePreCall();
            }
            checkRequestForComposite();
            if (composite && !pre_call_called) {
                return;
            }
            preparePostCall();
        }

        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
    };

    struct EtcdKeeperMultiResponse final : MultiResponse, EtcdKeeperResponse {};
  
    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperCreateResponse>();
        if (!compare_result)
        {
            std::cout << "ERROR" << std::endl;
            response->error = Error::ZNODEEXISTS;
            return response;
        }
        response->path_created = process_path;
        response->error = Error::ZNODEEXISTS;
        for (auto resp: responses) {
            if(ResponseOp::ResponseCase::kResponsePut == resp.response_case())
            {
                auto put_resp = resp.response_put();
                response->error = Error::ZOK;
                seqs[process_path] = 0;
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperRemoveResponse>();
        response->error = Error::ZNONODE;
        for (auto resp: responses) {
            if(ResponseOp::ResponseCase::kResponseDeleteRange == resp.response_case())
            {
                auto delete_range_resp = resp.response_delete_range();
                if (delete_range_resp.deleted())
                {
                    for (auto kv : delete_range_resp.prev_kvs())
                    {
                        if (kv.key() == path) {
                            response->error = Error::ZOK;
                            return response;
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperExistsResponse>();
        response->error = Error::ZNONODE;
        for (auto resp: responses) {
            if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == path) {
                        response->error = Error::ZOK;
                        return response;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperGetResponse>();
        response->error = Error::ZNONODE;
        if (compare_result) {
            for (auto resp: responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        std::cout << "P" << kv.key() << std::endl;
                        if (kv.key() == path) {
                            response->data = kv.value();
                            response->stat = Stat();
                            response->error = Error::ZOK;
                            return response;
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperSetResponse>();
        response->error = Error::ZNONODE;
        if (!compare_result) {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == path) {
                            response->error = Error::ZBADVERSION;
                            return response;
                        }
                    }
                }
            }
        }
        else
        {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponsePut == resp.response_case())
                {
                    auto put_resp = resp.response_put();
                    if (put_resp.prev_kv().key() == path) {
                        response->error = Error::ZOK;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperListResponse>();
        if (!compare_result)
        {
            response->error = Error::ZNONODE;
        }
        else
        {
            response->error = Error::ZOK;
        }
        for (auto resp : responses) {
            if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    std::cout << "KEY" << kv.key() << std::endl;
                    if (kv.key().rfind(path, 0) == 0)
                    {
                        for (auto kv : range_resp.kvs())
                        {
                            response->names.emplace_back(baseName(kv.key()));
                        }
                        response->error = Error::ZOK;
                        return response;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperCheckResponse>();
        if (compare_result) 
        {
            response->error = Error::ZOK;
        }
        else
        {
            response->error = Error::ZNODEEXISTS;
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == path) {
                            response->error = Error::ZBADVERSION;
                            return response;
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses_)
    { 
        auto response = std::make_shared<EtcdKeeperMultiResponse>();
        if (compare_result) {
            for (int i; i != etcd_requests.size(); i++) {
                response->responses.push_back(etcd_requests[i]->makeResponseFromResponses(compare_result, responses_));
            }
        }
        else
        {
            std::cout << "MULTI COMP" << etcd_requests.size() << std::endl;
            response->responses.reserve(etcd_requests.size());
            for (int i; i != etcd_requests.size(); i++) {
                std::cout << "MULTI COMPARE " << i << std::endl;
                Response resp;
                resp.error = Error::ZNONODE;
                response->responses.push_back(std::make_shared<Response>(resp));
            }
        }
        return response;
    }

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
        log = &Logger::get("EtcdKeeper");
        LOG_DEBUG(log, "INIT");

        std::string stripped_address = "localhost:2379";
        std::shared_ptr<Channel> channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        kv_stub_= KV::NewStub(channel);

        std::shared_ptr<Channel> watch_channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        watch_stub_= Watch::NewStub(watch_channel);

        stream_ = watch_stub_->AsyncWatch(&context_, &watch_cq_, (void*)WatchConnType::CONNECT);
        readWatchResponse();
 
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

        LOG_DEBUG(log, "DESTR");
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
                        LOG_DEBUG(log, "WATCH IN REQUEST");
                        bool list_watch = false;
                        if (dynamic_cast<const ListRequest *>(info.request.get())) {
                            list_watch = true;
                        }
                        callWatchRequest(info.request->getPath(), list_watch, watch_stub_, watch_cq_);
                    }

                    std::lock_guard lock(operations_mutex);
                    operations[info.request->xid] = info;

                    if (expired)
                        break;

                    info.request->addRootPath(root_path);

                    EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                    call->xid = info.request->xid;

                    info.request->prepareCall();
                    info.request->call(*call, kv_stub_, kv_cq_);
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
                LOG_DEBUG(log, "GOT TAG" << got_tag);

                GPR_ASSERT(ok);
                if (got_tag)
                {
                    auto call = static_cast<AsyncCall*>(got_tag);

                    XID xid = call->xid;

                    LOG_DEBUG(log, "XID" << xid);

                    auto it = operations.find(xid);
                    if (it == operations.end())
                        throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

                    request_info = std::move(it->second);
                    operations.erase(it);

                    if (!call->status.ok())
                    {
                        LOG_DEBUG(log, "RPC FAILED" << call->status.error_message());
                    }
                    else
                    {
                        LOG_DEBUG(log, "READ RPC RESPONSE");

                        if (!request_info.request->callRequired(got_tag))
                        {
                            response = request_info.request->makeResponseFromTag(got_tag);
                            response->removeRootPath(root_path);
                            if (request_info.callback)
                            request_info.callback(*response);
                        }
                        else
                        {
                            operations[request_info.request->xid] = request_info;
                            EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                            call->xid = request_info.request->xid;
                            request_info.request->prepareCall();
                            request_info.request->call(*call, kv_stub_, kv_cq_);
                        }
                    }
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

        void* got_tag;
        bool ok = false;

        try
        {
            while (watch_cq_.Next(&got_tag, &ok))
            {
                if (ok) {
                    std::cout << std::endl
                            << "**** Processing completion queue tag " << got_tag
                            << std::endl;
                    switch (static_cast<WatchConnType>(reinterpret_cast<long>(got_tag))) {
                    case WatchConnType::READ:
                        std::cout << "Read a new message." << std::endl;
                        readWatchResponse();
                        break;
                    case WatchConnType::WRITE:
                        std::cout << "Sending message (async)." << std::endl;
                        break;
                    case WatchConnType::CONNECT:
                        std::cout << "Server connected." << std::endl;
                        break;
                    case WatchConnType::WRITES_DONE:
                        std::cout << "Server disconnecting." << std::endl;
                        break;
                    case WatchConnType::FINISH:
                        // std::cout << "Client finish; status = "
                        //         << (finish_status_.ok() ? "ok" : "cancelled")
                        //         << std::endl;
                        context_.TryCancel();
                        watch_cq_.Shutdown();
                        break;
                    default:
                        std::cerr << "Unexpected tag " << got_tag << std::endl;
                        GPR_ASSERT(false);
                    }
                }
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
        LOG_DEBUG(log, "FINALIZE");

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
            {
                for (auto & path_watch : watches)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = ZSESSIONEXPIRED;
 
                    for (auto & callback : path_watch.second)
                    {
                        if (callback)
                        {
                            try
                            {
                                callback(response);
                            }
                            catch (...)
                            {
                                tryLogCurrentException(__PRETTY_FUNCTION__);
                            }
                        }
                    }
                }
 
                watches.clear();
            }
 
            RequestInfo info;
            while (requests_queue.tryPop(info))
            {
                if (info.callback)
                {
                    ResponsePtr response = info.request->makeResponse();
                    response->error = ZSESSIONEXPIRED;
                    try
                    {
                        info.callback(*response);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
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
                LOG_DEBUG(log, "XID" << next_xid);

                if (info.request->xid < 0)
                    throw Exception("XID overflow", ZSESSIONEXPIRED);
            }

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
        EtcdKeeperCreateRequest request;
        request.path = path;
        request.data = data;
        // request.process_path = request.getProcessPath();
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
        EtcdKeeperMultiRequest request(requests);
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperMultiRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
}
