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

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/color.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include <folly/MPMCQueue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest-matchers.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <SerializablePlanConbench.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <SystestGrpc.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Systest
{

std::vector<LoadedQueryPlan> loadFromSLTFile(
    const std::filesystem::path& testFilePath,
    const std::filesystem::path& workingDir,
    std::string_view testFileName,
    const std::filesystem::path& testDataDir)
{
    std::vector<LoadedQueryPlan> plans{};
    CLI::QueryConfig config{};
    SystestParser parser{};
    std::unordered_map<std::string, std::filesystem::path> sourceNamesToFilepath;
    std::unordered_map<std::string, SystestParser::Schema> sinkNamesToSchema{
        {"CHECKSUM",
         {{.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "S$Count"},
          {.type = DataTypeProvider::provideDataType(LogicalType::UINT64), .name = "S$Checksum"}}}};

    parser.registerSubstitutionRule({.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
    if (!parser.loadFile(testFilePath))
    {
        throw TestException("Could not successfully load test file://{}", testFilePath.string());
    }

    /// We create a map from sink names to their schema
    parser.registerOnSinkCallBack([&](SystestParser::Sink&& sinkParsed)
                                  { sinkNamesToSchema.insert_or_assign(sinkParsed.name, sinkParsed.fields); });

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SystestParser::CSVSource&& source)
        {
            config.logical.emplace_back(
                CLI::LogicalSource{
                    .name = source.name,
                    .schema = [&source]()
                    {
                        std::vector<CLI::SchemaField> schema;
                        for (const auto& [type, name] : source.fields)
                        {
                            schema.emplace_back(name, type);
                        }
                        return schema;
                    }()});

            config.physical.emplace_back(
                CLI::PhysicalSource{
                    .logical = source.name,
                    .parserConfig = {{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", ","}},
                    .sourceConfig
                    = {{"type", "File"}, {"filePath", source.csvFilePath}, {"numberOfBuffersInSourceLocalBufferPool", "-1"}}});
            sourceNamesToFilepath[source.name] = source.csvFilePath;
        });

    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& source)
        {
            static uint64_t sourceIndex = 0;

            config.logical.emplace_back(
                CLI::LogicalSource{
                    .name = source.name,
                    .schema = [&source]()
                    {
                        std::vector<CLI::SchemaField> schema;
                        for (const auto& [type, name] : source.fields)
                        {
                            schema.emplace_back(name, type);
                        }
                        return schema;
                    }()});

            const auto sourceFile = Query::sourceFile(workingDir, testFileName, sourceIndex++);
            sourceNamesToFilepath[source.name] = sourceFile;
            config.physical.emplace_back(
                CLI::PhysicalSource{
                    .logical = source.name,
                    .parserConfig = {{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", ","}},
                    .sourceConfig = {{"type", "File"}, {"filePath", sourceFile}, {"numberOfBuffersInSourceLocalBufferPool", "-1"}}});
            {
                std::ofstream testFile(sourceFile);
                if (!testFile.is_open())
                {
                    throw TestException("Could not open source file \"{}\"", sourceFile);
                }

                for (const auto& tuple : source.tuples)
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
            }

            NES_INFO("Written in file: {}. Number of Tuples: {}", sourceFile, source.tuples.size());
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SystestParser::Query&& query)
        {
            /// For system level tests, a single file can hold arbitrary many tests. We need to generate a unique sink name for
            /// every test by counting up a static query number. We then emplace the unique sinks in the global (per test file) query config.
            static size_t currentQueryNumber = 0;
            static std::string currentTestFileName;

            /// We reset the current query number once we see a new test file
            if (currentTestFileName != testFileName)
            {
                currentTestFileName = testFileName;
                currentQueryNumber = 0;
            }
            else
            {
                ++currentQueryNumber;
            }

            /// We expect at least one sink to be defined in the test file
            if (sinkNamesToSchema.empty())
            {
                throw TestException("No sinks defined in test file: {}", testFileName);
            }

            /// We have to get all sink names from the query and then create custom paths for each sink.
            /// The filepath can not be the sink name, as we might have multiple queries with the same sink name, i.e., sink20Booleans in FunctionEqual.test
            /// We assume:
            /// - the INTO keyword is the last keyword in the query
            /// - the sink name is the last word in the INTO clause
            const auto sinkName = [&query]() -> std::string
            {
                const auto intoClause = query.find("INTO");
                if (intoClause == std::string::npos)
                {
                    NES_ERROR("INTO clause not found in query: {}", query);
                    return "";
                }
                const auto intoLength = std::string("INTO").length();
                auto trimmedSinkName = std::string(NES::Util::trimWhiteSpaces(query.substr(intoClause + intoLength)));

                /// As the sink name might have a semicolon at the end, we remove it
                if (trimmedSinkName.back() == ';')
                {
                    trimmedSinkName.pop_back();
                }
                return trimmedSinkName;
            }();

            if (sinkName.empty() or not sinkNamesToSchema.contains(sinkName))
            {
                throw UnknownSinkType("Failed to find sink name <{}>", sinkName);
            }


            /// Replacing the sinkName with the created unique sink name
            const auto sinkForQuery = sinkName + std::to_string(currentQueryNumber);
            query = std::regex_replace(query, std::regex(sinkName), sinkForQuery);

            /// Adding the sink to the sink config, such that we can create a fully specified query plan
            const auto resultFile = Query::resultFile(workingDir, testFileName, currentQueryNumber);

            if (sinkName == "CHECKSUM")
            {
                auto sink = CLI::Sink{.name = sinkName, .type = "Checksum", .config = {std::make_pair("filePath", resultFile)}};
                config.sinks.emplace(sinkForQuery, std::move(sink));
            }
            else
            {
                auto sinkCLI = CLI::Sink{
                    .name = sinkForQuery,
                    .type = "File",
                    .config
                    = {std::make_pair("inputFormat", "CSV"), std::make_pair("filePath", resultFile), std::make_pair("append", "false")}};
                config.sinks.emplace(sinkForQuery, std::move(sinkCLI));
            }

            config.query = query;
            auto plan = createFullySpecifiedQueryPlan(config);
            std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCountForQuery;
            for (const auto& logicalSource : NES::getOperatorByType<SourceDescriptorLogicalOperator>(*plan))
            {
                const auto sourceName = logicalSource.getSourceDescriptor()->logicalSourceName;
                if (sourceNamesToFilepath.contains(sourceName))
                {
                    auto& entry = sourceNamesToFilepathAndCountForQuery[sourceName];
                    entry = {sourceNamesToFilepath.at(sourceName), entry.second + 1};
                    continue;
                }
                throw CannotLoadConfig("SourceName {} does not exist in sourceNamesToFilepathAndCount!");
            }
            INVARIANT(not sourceNamesToFilepathAndCountForQuery.empty(), "sourceNamesToFilepathAndCountForQuery should not be empty!");
            plans.emplace_back(*plan, query, sinkNamesToSchema[sinkName], sourceNamesToFilepathAndCountForQuery);
        });
    try
    {
        parser.parse();
    }
    catch (Exception& exception)
    {
        tryLogCurrentException();
        exception.what() += fmt::format("Could not successfully parse test file://{}", testFilePath.string());
        throw exception;
    }
    return plans;
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<Query>& queries,
    const uint64_t numConcurrentQueries,
    const Configuration::SingleNodeWorkerConfiguration& configuration)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::Synchronized<std::vector<std::shared_ptr<RunningQuery>>> runningQueries;
    SingleNodeWorker worker(configuration);

    std::atomic<size_t> queriesToResultCheck{0};
    std::atomic<size_t> queryFinishedCounter{0};
    const auto totalQueries = queries.size();
    std::atomic finishedProducing{false};

    std::mutex mtx;
    std::condition_variable cv;

    std::jthread producer(
        [&queries, &queriesToResultCheck, &worker, &numConcurrentQueries, &runningQueries, &finishedProducing, &mtx, &cv](
            std::stop_token stopToken)
        {
            for (auto& query : queries)
            {
                {
                    std::unique_lock lock(mtx);
                    cv.wait(
                        lock,
                        [&stopToken, &queriesToResultCheck, &numConcurrentQueries]
                        { return stopToken.stop_requested() or queriesToResultCheck.load() < numConcurrentQueries; });
                    if (stopToken.stop_requested())
                    {
                        finishedProducing = true;
                        return;
                    }
                    const auto queryId = worker.registerQuery(query.queryPlan, nullptr, nullptr);
                    if (queryId == INVALID_QUERY_ID)
                    {
                        throw QueryInvalid("Received an invalid query id from the worker");
                    }
                    worker.startQuery(queryId);

                    auto runningQueryPtr = std::make_unique<RunningQuery>(query, QueryId(queryId));
                    runningQueries.wlock()->emplace_back(std::move(runningQueryPtr));

                    /// We are waiting if we have reached the maximum number of concurrent queries
                    queriesToResultCheck.fetch_add(1);
                    while (queriesToResultCheck.load() == numConcurrentQueries)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or queriesToResultCheck > 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        {
            auto runningQueriesLock = runningQueries.wlock();
            for (auto runningQueriesIter = runningQueriesLock->begin(); runningQueriesIter != runningQueriesLock->end();)
            {
                const auto& runningQuery = *runningQueriesIter;
                if (runningQuery)
                {
                    const auto querySummary = worker.getQuerySummary(QueryId(runningQuery->queryId));
                    if (not querySummary.has_value())
                    {
                        /// Query Summary is not yet available
                        continue;
                    }

                    if (const auto queryStatus = querySummary->currentStatus;
                        queryStatus == QueryStatus::Stopped or queryStatus == QueryStatus::Failed)
                    {
                        worker.unregisterQuery(QueryId(runningQuery->queryId));

                        std::optional<std::string> errorMessage;
                        if (queryStatus == QueryStatus::Failed)
                        {
                            errorMessage
                                = querySummary->runs.back().error.transform([](auto ex) { return ex.what(); }).value_or("No Error Message");
                        }
                        else
                        {
                            errorMessage = checkResult(*runningQuery);
                        }
                        runningQuery->passed = not errorMessage.has_value();
                        runningQuery->querySummary = querySummary.value();
                        printQueryResultToStdOut(
                            *runningQuery, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries, "");
                        if (errorMessage.has_value())
                        {
                            failedQueries.wlock()->emplace_back(*runningQuery);
                        }
                        runningQueriesIter = runningQueriesLock->erase(runningQueriesIter);
                        queriesToResultCheck.fetch_sub(1);
                        cv.notify_one();
                        continue;
                    }
                }
                ++runningQueriesIter;
            }
        }
    }

    return failedQueries.copy();
}

std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, const uint64_t numConcurrentQueries, const std::string& serverURI)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::Synchronized<std::vector<std::shared_ptr<RunningQuery>>> runningQueries;
    const GRPCClient client(CreateChannel(serverURI, grpc::InsecureChannelCredentials()));

    std::atomic<size_t> runningQueryCount{0};
    std::atomic<size_t> queryFinishedCounter{0};
    const auto totalQueries = queries.size();
    std::atomic finishedProducing{false};

    std::mutex mtx;
    std::condition_variable cv;

    std::jthread producer(
        [&queries, &runningQueryCount, &client, &numConcurrentQueries, &runningQueries, &finishedProducing, &mtx, &cv](
            std::stop_token stopToken)
        {
            for (auto& query : queries)
            {
                {
                    std::unique_lock lock(mtx);
                    cv.wait(lock, [&] { return stopToken.stop_requested() or runningQueryCount.load() < numConcurrentQueries; });
                    if (stopToken.stop_requested())
                    {
                        finishedProducing = true;
                        return;
                    }

                    const auto queryId = client.registerQuery(query.queryPlan);
                    client.start(queryId);
                    auto runningQueryPtr = std::make_unique<RunningQuery>(query, QueryId(queryId));
                    runningQueries.wlock()->emplace_back(std::move(runningQueryPtr));

                    /// We are waiting if we have reached the maximum number of concurrent queries
                    runningQueryCount.fetch_add(1);
                    while (runningQueryCount.load() <= numConcurrentQueries)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or runningQueryCount > 0)
    {
        auto runningQueriesLock = runningQueries.wlock();
        for (auto runningQueriesIter = runningQueriesLock->begin(); runningQueriesIter != runningQueriesLock->end();)
        {
            const auto& runningQuery = *runningQueriesIter;
            if (runningQuery and (client.status(runningQuery->queryId.getRawValue()).currentStatus == QueryStatus::Stopped or client.status(runningQuery->queryId.getRawValue()).currentStatus == QueryStatus::Failed))
            {
                client.unregister(runningQuery->queryId.getRawValue());
                runningQuery->querySummary = client.status(runningQuery->queryId.getRawValue());
                auto errorMessage = checkResult(*runningQuery);
                runningQuery->passed = not errorMessage.has_value();

                printQueryResultToStdOut(*runningQuery, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries, "");
                if (errorMessage.has_value())
                {
                    failedQueries.wlock()->emplace_back(*runningQuery);
                }
                runningQueriesIter = runningQueriesLock->erase(runningQueriesIter);
                runningQueryCount.fetch_sub(1);
                cv.notify_one();
            }
            else
            {
                ++runningQueriesIter;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    return failedQueries.copy();
}

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

    /// Serialize operators as protobuf objects
    for (auto rootOp : plan.rootOperators)
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

        /// Serialize the logical plan
        /// We can tell that visualizing queries is activated, by checking if the pipelinePlanJson vector is filled
        if (!pipelinePlanJson.empty())
        {
            nlohmann::json logicalPlanJson;
            serializeQueryToJson(queryRan.query.queryPlan, logicalPlanJson);
            resultJson.push_back(
                {{"query name", queryRan.query.resultFile().stem()},
                 {"time", executionTimeInSeconds},
                 {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
                 {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
                 {"serializedLogicalPlan", logicalPlanJson},
                 {"serializedPipelinePlan", pipelinePlanJson[i]}});
        }
        else
        {
            resultJson.push_back({
                {"query name", queryRan.query.resultFile().stem()},
                {"time", executionTimeInSeconds},
                {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
                {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
            });
        }
    }
    return failedQueries;
}

QuerySummary waitForQueryTermination(SingleNodeWorker& worker, QueryId queryId)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        const auto summary = worker.getQuerySummary(queryId);
        if (summary)
        {
            if (summary->currentStatus == QueryStatus::Stopped)
            {
                return *summary;
            }
        }
    }
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<Query>& queries,
    const Configuration::SingleNodeWorkerConfiguration& configuration,
    nlohmann::json& resultJson,
    bool visualizePlans)
{
    /// We create a vector with pipeline plan serializations.
    /// The vector will remain empty if visualizePlans is not set.
    std::vector<nlohmann::json> pipelinePlanSerializations;
    SingleNodeWorker worker(configuration);
    std::vector<RunningQuery> ranQueries;
    std::size_t queryFinishedCounter = 0;
    const auto totalQueries = queries.size();
    for (const auto& queryToRun : queries)
    {
        auto pipelinePlanJson = nlohmann::json();
        /// In this map, we store pointers to the incoming tuple counters for each intermediate pipeline stage
        /// After the query finished running, we can obtain the incoming tuples per pipeline for additional benchmarking data
        std::unordered_map<uint64_t, std::shared_ptr<std::atomic<uint64_t>>> incomingTuplesMap;

        /// Pass the pipelineJson and the incoming tuples map into the register query function, if we want to visualize the query plan on Conbench
        /// Otherwise pass 2 nullptrs
        /// Passing raw pointers to the json and the map is a hotfix for not being able to set std::optional<class&> as arguments
        const auto queryId = visualizePlans ? worker.registerQuery(queryToRun.queryPlan, &pipelinePlanJson, &incomingTuplesMap)
                                            : worker.registerQuery(queryToRun.queryPlan, nullptr, nullptr);

        RunningQuery currentRunningQuery(queryToRun, queryId);
        {
            /// Measuring the time it takes from registering the query till unregistering / completion
            worker.startQuery(queryId);
            const auto summary = waitForQueryTermination(worker, queryId);
            if (summary.runs.back().error.has_value())
            {
                fmt::println(std::cerr, "Query {} has failed with: {}", queryId, summary.runs.back().error->what());
                continue;
            }
            worker.unregisterQuery(queryId);
            currentRunningQuery.querySummary = summary;
        }

        /// Add incoming tuples as attribute for intermediate and sink pipeline and emplace filled pipelinePlanJson in the vector of all the jsons
        if (visualizePlans)
        {
            /// Since the number of incoming tuples for the sink pipeline has not been accumulated yet, we still need to get them
            /// We can get this number by either checking the first field of the checksum sink or counting the results of any other sink
            std::regex checksumPattern(R"(CHECKSUM\d*)");
            LogicalOperator rootOp = queryToRun.queryPlan.rootOperators[0];
            auto sinkOp = rootOp.tryGet<SinkLogicalOperator>();
            if (!sinkOp)
            {
                NES_ERROR("Root operator of the logical plan should always be a sink.")
            }
            std::string sinkName = sinkOp.value().sinkName;
            std::cout << sinkName << std::endl;
            uint64_t finalTupleCount = 0;
            std::vector<std::string> results = loadQueryResult(queryToRun)->result;

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

        /// Getting the size and no. tuples of all input files to pass this information to currentRunningQuery.bytesProcessed
        size_t bytesProcessed = 0;
        size_t tuplesProcessed = 0;
        for (const auto& [sourcePath, sourceOccurrencesInQuery] : queryToRun.sourceNamesToFilepathAndCount | std::views::values)
        {
            bytesProcessed += (std::filesystem::file_size(sourcePath) * sourceOccurrencesInQuery);

            /// Counting the lines, i.e., \n in the sourcePath
            std::ifstream inFile(sourcePath);
            tuplesProcessed
                += std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n') * sourceOccurrencesInQuery;
        }
        currentRunningQuery.bytesProcessed = bytesProcessed;
        currentRunningQuery.tuplesProcessed = tuplesProcessed;

        auto errorMessage = checkResult(currentRunningQuery);
        currentRunningQuery.passed = not errorMessage.has_value();
        const auto queryPerformanceMessage
            = fmt::format(" in {} ({})", currentRunningQuery.getElapsedTime(), currentRunningQuery.getThroughput());
        printQueryResultToStdOut(
            currentRunningQuery, errorMessage.value_or(""), queryFinishedCounter, totalQueries, queryPerformanceMessage);
        ranQueries.emplace_back(currentRunningQuery);

        queryFinishedCounter += 1;
    }

    return serializeExecutionResults(ranQueries, resultJson, pipelinePlanSerializations);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    const size_t queryCounter,
    const size_t totalQueries,
    const std::string_view queryPerformanceMessage)
{
    const auto queryNameLength = runningQuery.query.name.size();
    const auto queryNumberAsString = std::to_string(runningQuery.query.queryIdInFile + 1);
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.query.name << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (runningQuery.passed)
    {
        std::cout << "PASSED" << queryPerformanceMessage << '\n';
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.query.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}
}
