/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <SystestRunner.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <QueryManager/EmbeddedWorkerQueryManager.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/MPMCQueue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest-matchers.h>
#include <nlohmann/json.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SerializablePlanConbench.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
namespace
{
template <typename ErrorCallable>
void reportResult(
    std::shared_ptr<RunningQuery>& runningQuery,
    std::size_t& finishedCount,
    std::size_t total,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    ErrorCallable&& errorBuilder)
{
    std::string msg = errorBuilder();
    runningQuery->passed = msg.empty();
    printQueryResultToStdOut(*runningQuery, msg, finishedCount++, total, "");
    if (!msg.empty())
    {
        failed.push_back(runningQuery);
    }
}

bool passes(const std::shared_ptr<RunningQuery>& runningQuery)
{
    return runningQuery->passed;
}

void processQueryWithError(
    std::shared_ptr<RunningQuery> runningQuery,
    std::size_t& finished,
    const size_t numQueries,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    const std::optional<Exception>& exception)
{
    runningQuery->exception = exception;
    reportResult(
        runningQuery,
        finished,
        numQueries,
        failed,
        [&]
        {
            if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError)
                and std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code
                    == runningQuery->exception->code())
            {
                return std::string{};
            }
            return fmt::format("unexpected parsing error: {}", *runningQuery->exception);
        });
}

}

/// NOLINTBEGIN(readability-function-cognitive-complexity)
std::vector<RunningQuery>
runQueries(const std::vector<SystestQuery>& queries, const uint64_t numConcurrentQueries, QuerySubmitter& querySubmitter)
{
    std::queue<SystestQuery> pending;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        pending.push(*it);
    }

    std::unordered_map<QueryId, std::shared_ptr<RunningQuery>> active;
    std::vector<std::shared_ptr<RunningQuery>> failed;
    std::size_t finished = 0;

    const auto startMoreQueries = [&] -> bool
    {
        bool hasOneMoreQueryToStart = false;
        while (active.size() < numConcurrentQueries and not pending.empty())
        {
            SystestQuery nextQuery = std::move(pending.front());
            pending.pop();

            if (nextQuery.planInfoOrException.has_value())
            {
                /// Registration
                if (auto reg = querySubmitter.registerQuery(nextQuery.planInfoOrException.value().queryPlan, nullptr, nullptr))
                {
                    hasOneMoreQueryToStart = true;
                    querySubmitter.startQuery(*reg);
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg));
                }
                else
                {
                    processQueryWithError(std::make_shared<RunningQuery>(nextQuery), finished, queries.size(), failed, {reg.error()});
                }
            }
            else
            {
                /// There was an error during query parsing, report the result and don't register the query
                processQueryWithError(
                    std::make_shared<RunningQuery>(nextQuery), finished, queries.size(), failed, {nextQuery.planInfoOrException.error()});
            }
        }
        return hasOneMoreQueryToStart;
    };

    while (startMoreQueries() or not(active.empty() and pending.empty()))
    {
        for (const auto& summary : querySubmitter.finishedQueries())
        {
            auto it = active.find(summary.queryId);
            if (it == active.end())
            {
                throw TestException("received unregistered queryId: {}", summary.queryId);
            }

            auto& runningQuery = it->second;

            if (summary.currentStatus == QueryStatus::Failed)
            {
                INVARIANT(summary.runs.back().error, "A query that failed must have a corresponding error.");
                processQueryWithError(it->second, finished, queries.size(), failed, summary.runs.back().error);
            }
            else
            {
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError))
                        {
                            return fmt::format(
                                "expected error {} but query succeeded",
                                std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code);
                        }
                        runningQuery->querySummary = summary;
                        if (auto err = checkResult(*runningQuery))
                        {
                            return *err;
                        }
                        return std::string{};
                    });
            }
            active.erase(it);
        }
    }

    auto failedViews = failed | std::views::filter(std::not_fn(passes)) | std::views::transform([](auto& p) { return *p; });
    return {failedViews.begin(), failedViews.end()};
}

/// NOLINTEND(readability-function-cognitive-complexity)

namespace
{
/// Serializes a logical operator to a protobuf representation. Will serialize the children beforehand and not serialize the operator, if it was already visited.
void serializeOperatorToJson(
    LogicalPlan plan, LogicalOperator& op, std::vector<uint64_t>& foundOps, std::vector<SerializableLogicalOperatorNode>& operatorList)
{
    if (const auto id = std::ranges::find(foundOps, op.getId().getRawValue()); id == foundOps.end())
    {
        SerializableLogicalOperatorNode serializedOperator;
        /// If we have not visited this op before, we visit it now and create its entry in the operatorList
        uint64_t opId = op.getId().getRawValue();
        serializedOperator.set_id(opId);
        foundOps.emplace_back(opId);
        std::string opType(op.getName());
        serializedOperator.set_node_type(opType);

        /// Get all parent operator ids of this operator
        std::vector<LogicalOperator> parents = getParents(plan, op);
        std::vector<uint64_t> parentIds;
        for (const auto& parent : parents)
        {
            serializedOperator.add_outputs(parent.getId().getRawValue());
        }

        std::vector<uint64_t> childrenIds;
        for (auto child : op.getChildren())
        {
            /// Serialize children and get their ids
            serializeOperatorToJson(plan, child, foundOps, operatorList);
            serializedOperator.add_inputs(child.getId().getRawValue());
        }

        std::string opLabel = op.explain(ExplainVerbosity::Short);
        serializedOperator.set_label(opLabel);
        operatorList.emplace_back(serializedOperator);
    }
}

/// Function to start off the json serialization of the logical query plan operators
/// Takes each sink of the query and recursively adds the serializations of the sink and its children to the serialization list.
/// Serialization of a single operator happens as protobuf serialization. The list of protobuf objects is then transformed to a list of json objects.
void serializeQueryToJson(LogicalPlan plan, nlohmann::json& resultJson)
{
    std::vector<uint64_t> foundOps = std::vector<uint64_t>();
    std::vector<SerializableLogicalOperatorNode> operators = {};
    for (auto rootOp : plan.getRootOperators())
    {
        serializeOperatorToJson(plan, rootOp, foundOps, operators);
    }

    /// Include empty lists in the json, like an empty inputs list for a source
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_fields_with_no_presence = true;

    /// Transform protobuf list to json list
    for (auto serializedOp : operators)
    {
        std::string op_string;
        absl::Status status = google::protobuf::util::MessageToJsonString(serializedOp, &op_string, options);
        if (!status.ok())
        {
            NES_ERROR("Error while trying to obtain json string of logical plan for query {}", plan.getQueryId());
        }
        const nlohmann::json opJson = nlohmann::json::parse(op_string);
        resultJson.push_back(opJson);
    }
}

/// Adds the "incomingTuples" field to every pipeline in resultJson, which contains the number of incoming tuples for each pipeline
/// The source pipelines get a placeholder value 0, since their incoming tuple value is not relevant for visualization
/// For each intermediate pipeline, the number of incoming tuples is contained in the respective atomic counter in the incomingTuplesMap. We access it via the pipeline id
/// For the sink pipeline, the number of incoming tuples is contained in finalTupleCount
void addIncomingTuplesToPipelinePlan(
    nlohmann::json& resultJson,
    std::unordered_map<uint64_t, std::shared_ptr<std::atomic<uint64_t>>> incomingTuplesMap,
    uint64_t finalTupleCount)
{
    for (nlohmann::json& pipeline : resultJson)
    {
        /// We have 3 options for a pipeline: Either it's a source pipeline (predeccessors are empty), its the sink (successors are empty) are it's an intermediate pipeline
        if (pipeline["predecessors"].empty())
        {
            /// The source pipeline gets a placeholder value as incoming tuples, since there is no incoming edge to it in the graph
            pipeline["incomingTuples"] = 0;
        }
        else if (pipeline["successors"].empty())
        {
            /// This is our sink. FinalTupleCount contains it's incoming tuple value
            pipeline["incomingTuples"] = finalTupleCount;
        }
        else
        {
            /// Intermediate pipeline. The numbers of incoming tuples should be in the map
            if (uint64_t pipelineId = pipeline["pipelineId"]; incomingTuplesMap.contains(pipelineId))
            {
                uint64_t incomingTuples = incomingTuplesMap[pipelineId]->load(std::memory_order_relaxed);
                pipeline["incomingTuples"] = incomingTuples;
            }
            else
            {
                NES_ERROR("Could not find incoming tuples for pipeline {}", pipelineId);
            }
        }
    }
}

/// Serializes the query results in the resultJson object
/// Captures runtime and bytes per ms
/// Optionally captures json serializations of logical and pipeline query plan to visualize on the Conbench server
std::vector<RunningQuery> serializeExecutionResults(
    const std::vector<RunningQuery>& queries, nlohmann::json& resultJson, std::vector<nlohmann::json>& pipelinePlanJson)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& [i, queryRan] : views::enumerate(queries))
    {
        if (!queryRan.passed)
        {
            failedQueries.emplace_back(queryRan);
        }
        const auto executionTimeInSeconds = queryRan.getElapsedTime().count();
        std::expected<SystestQuery::PlanInfo, Exception> planInfo = queryRan.systestQuery.planInfoOrException;
        /// If we cannot access the logical plan of the query, we cannot serialize it to json.
        /// Therefore, we then skip the serialization part.
        if (!pipelinePlanJson.empty() && planInfo)
        {
            const auto logicalPlan = planInfo.value().queryPlan;
            nlohmann::json logicalPlanJson;
            serializeQueryToJson(logicalPlan, logicalPlanJson);
            resultJson.push_back(
                {{"query name", queryRan.systestQuery.testName},
                 {"time", executionTimeInSeconds},
                 {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
                 {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
                 {"serializedLogicalPlan", logicalPlanJson},
                 {"serializedPipelinePlan", pipelinePlanJson[i]}});
        }
        else
        {
            resultJson.push_back({
                {"query name", queryRan.systestQuery.testName},
                {"time", executionTimeInSeconds},
                {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
                {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
            });
        }
    }
    return failedQueries;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries,
    const SingleNodeWorkerConfiguration& configuration,
    nlohmann::json& resultJson,
    bool visualizePlans)
{
    auto worker = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
    QuerySubmitter submitter(std::move(worker));
    std::vector<std::shared_ptr<RunningQuery>> ranQueries;
    /// We create a vector with pipeline plan serializations.
    /// The vector will remain empty if visualizePlans is not set.
    std::vector<nlohmann::json> pipelinePlanSerializations;
    std::size_t queryFinishedCounter = 0;
    const auto totalQueries = queries.size();
    for (const auto& queryToRun : queries)
    {
        if (not queryToRun.planInfoOrException.has_value())
        {
            fmt::println("skip failing query: {}", queryToRun.testName);
            continue;
        }
        auto pipelinePlanJson = nlohmann::json();
        /// In this map, we store pointers to the incoming tuple counters for each intermediate pipeline stage
        /// After the query finished running, we can obtain the incoming tuples per pipeline for additional benchmarking data
        std::unordered_map<uint64_t, std::shared_ptr<std::atomic<uint64_t>>> incomingTuplesMap;

        /// Pass the pipelineJson and the incoming tuples map into the register query function, if we want to visualize the query plan on Conbench
        /// Otherwise pass 2 nullptrs
        /// Passing raw pointers to the json and the map is a hotfix for not being able to set std::optional<class&> as arguments
        const auto registrationResult = visualizePlans
            ? submitter.registerQuery(queryToRun.planInfoOrException.value().queryPlan, &pipelinePlanJson, &incomingTuplesMap)
            : submitter.registerQuery(queryToRun.planInfoOrException.value().queryPlan, nullptr, nullptr);

        auto queryId = registrationResult.value();

        auto runningQueryPtr = std::make_shared<RunningQuery>(queryToRun, queryId);
        runningQueryPtr->passed = false;
        ranQueries.emplace_back(runningQueryPtr);
        submitter.startQuery(queryId);
        const auto summary = submitter.finishedQueries().at(0);

        if (summary.runs.empty() or summary.runs.back().error.has_value())
        {
            fmt::println(std::cerr, "Query {} has failed with: {}", queryId, summary.runs.back().error->what());
            continue;
        }
        runningQueryPtr->querySummary = summary;

        /// Add incoming tuples as attribute for intermediate and sink pipeline and emplace filled pipelinePlanJson in the vector of all the jsons
        if (visualizePlans)
        {
            /// Since the number of incoming tuples for the sink pipeline has not been accumulated yet, we still need to get them.
            /// We can get this number by either checking the first field of the checksum sink or counting the results of any other sink
            std::regex checksumPattern(R"(CHECKSUM\d*)");
            std::expected<SystestQuery::PlanInfo, Exception> planInfo = queryToRun.planInfoOrException;

            /// Only if the logical plan can be accessed, we may continue. Otherwise, the pipeline plan won't be integrated in the BenchmarkResult json anyway.
            if (planInfo)
            {
                const auto logicalPlan = planInfo.value().queryPlan;
                LogicalOperator rootOp = logicalPlan.getRootOperators()[0];
                auto sinkOp = rootOp.tryGet<SinkLogicalOperator>();
                if (!sinkOp)
                {
                    NES_ERROR("Root operator of the logical plan should always be a sink.")
                }
                std::string sinkName = sinkOp.value().getSinkName();
                uint64_t finalTupleCount = 0;

                std::optional<std::vector<std::string>> queryResult = loadResult(queryToRun);
                if (not queryResult)
                {
                    NES_ERROR("Could not load query result.");
                }

                std::vector<std::string> results = queryResult.value();

                if (std::regex_match(sinkName, checksumPattern))
                {
                    /// The sink is a checksum sink, we need to read the first field of the first result to obtain the number of arrived tuples
                    const std::string result = results[0];
                    /// Get the arrived tuples by parsing the string to the first komma
                    std::stringstream ss(result);
                    std::string tupleCountAsString;
                    std::getline(ss, tupleCountAsString, ',');
                    finalTupleCount = std::stoull(tupleCountAsString);
                }
                else
                {
                    /// A normal sink, we count the number of arrived tuples by looking at the size of teh result vector
                    finalTupleCount = results.size();
                }

                /// Enrich pipeline plan json with incoming tuples for every pipeline
                addIncomingTuplesToPipelinePlan(pipelinePlanJson, incomingTuplesMap, finalTupleCount);
                pipelinePlanSerializations.push_back(pipelinePlanJson);
            }
        }

        /// Getting the size and no. tuples of all input files to pass this information to currentRunningQuery.bytesProcessed
        size_t bytesProcessed = 0;
        size_t tuplesProcessed = 0;
        for (const auto& [sourcePath, sourceOccurrencesInQuery] :
             queryToRun.planInfoOrException.value().sourcesToFilePathsAndCounts | std::views::values)
        {
            if (not(std::filesystem::exists(sourcePath.getRawValue()) and sourcePath.getRawValue().has_filename()))
            {
                NES_ERROR("Source path is empty or does not exist.");
                bytesProcessed = 0;
                tuplesProcessed = 0;
                break;
            }

            bytesProcessed += (std::filesystem::file_size(sourcePath.getRawValue()) * sourceOccurrencesInQuery);

            /// Counting the lines, i.e., \n in the sourcePath
            std::ifstream inFile(sourcePath.getRawValue());
            tuplesProcessed
                += std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n') * sourceOccurrencesInQuery;
        }
        ranQueries.back()->bytesProcessed = bytesProcessed;
        ranQueries.back()->tuplesProcessed = tuplesProcessed;

        auto errorMessage = checkResult(*ranQueries.back());
        ranQueries.back()->passed = not errorMessage.has_value();
        const auto queryPerformanceMessage
            = fmt::format(" in {} ({})", ranQueries.back()->getElapsedTime(), ranQueries.back()->getThroughput());
        printQueryResultToStdOut(
            *ranQueries.back(), errorMessage.value_or(""), queryFinishedCounter, totalQueries, queryPerformanceMessage);

        queryFinishedCounter += 1;
    }

    return serializeExecutionResults(
        ranQueries | std::views::transform([](const auto& query) { return *query; }) | std::ranges::to<std::vector>(),
        resultJson,
        pipelinePlanSerializations);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    const size_t queryCounter,
    const size_t totalQueries,
    const std::string_view queryPerformanceMessage)
{
    const auto queryNameLength = runningQuery.systestQuery.testName.size();
    const auto queryNumberAsString = runningQuery.systestQuery.queryIdInFile.toString();
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.systestQuery.testName << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0')
              << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (runningQuery.passed)
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.systestQuery.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries, const uint64_t numConcurrentQueries, const SingleNodeWorkerConfiguration& configuration)
{
    auto embeddedQueryManager = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
    QuerySubmitter submitter(std::move(embeddedQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter);
}

std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<SystestQuery>& queries, const uint64_t numConcurrentQueries, const std::string& serverURI)
{
    auto remoteQueryManager = std::make_unique<GRPCQueryManager>(CreateChannel(serverURI, grpc::InsecureChannelCredentials()));
    QuerySubmitter submitter(std::move(remoteQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter);
}

}
