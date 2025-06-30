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

#include <cstddef>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{

PipelinedQueryPlan::PipelinedQueryPlan(QueryId id, Nautilus::Configurations::ExecutionMode executionMode)
    : queryId(id), executionMode(executionMode) { };

static void printPipelineRecursive(const Pipeline* pipeline, std::ostream& os, int indentLevel)
{
    const std::string indent(indentLevel * 2, ' ');
    os << indent << *pipeline << "\n";
    for (const auto& succ : pipeline->getSuccessors())
    {
        os << indent << "Successor Pipeline:\n";
        printPipelineRecursive(succ.get(), os, indentLevel + 1);
    }
}

std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& plan)
{
    os << "PipelinedQueryPlan for Query: " << plan.getQueryId() << "\n";
    os << "Number of root pipelines: " << plan.getPipelines().size() << "\n";
    for (size_t i = 0; i < plan.getPipelines().size(); ++i)
    {
        os << "------------------\n";
        os << "Root Pipeline " << i << ":\n";
        printPipelineRecursive(plan.getPipelines()[i].get(), os, 1);
    }
    return os;
}

void PipelinedQueryPlan::removePipeline(Pipeline& pipeline)
{
    pipeline.clearSuccessors();
    pipeline.clearPredecessors();
    std::erase_if(pipelines, [&pipeline](const auto& ptr) { return ptr->getPipelineId() == pipeline.getPipelineId(); });
}

std::vector<std::shared_ptr<Pipeline>> PipelinedQueryPlan::getSourcePipelines() const
{
    return std::views::filter(pipelines, [](const auto& pipelinePtr) { return pipelinePtr->isSourcePipeline(); })
        | std::ranges::to<std::vector>();
}

QueryId PipelinedQueryPlan::getQueryId() const
{
    return queryId;
}

Nautilus::Configurations::ExecutionMode PipelinedQueryPlan::getExecutionMode() const
{
    return executionMode;
}

const std::vector<std::shared_ptr<Pipeline>>& PipelinedQueryPlan::getPipelines() const
{
    return pipelines;
}

void PipelinedQueryPlan::addPipeline(const std::shared_ptr<Pipeline>& pipeline)
{
    pipeline->setExecutionMode(executionMode);
    pipelines.push_back(pipeline);
}

/// Store pipeline id, predecessor ids, successor ids and list of serialized physical operators of a pipeline. Afterward, serialize all unseen successors.
void serializePipelineRecursive(
    nlohmann::json* resultJson, const std::shared_ptr<Pipeline>& pipeline, std::vector<uint64_t>& visitedPipelines)
{
    uint64_t id = pipeline->getPipelineId().getRawValue();
    visitedPipelines.push_back(id);

    std::vector<uint64_t> successorIds;
    for (const auto& successor : pipeline->getSuccessors())
    {
        successorIds.push_back(successor->getPipelineId().getRawValue());
    }

    std::vector<uint64_t> predecessorIds;
    for (const auto& predecessor : pipeline->getPredecessors())
    {
        /// Since the predecessor might have been erased, we need to check if it still exists with lock.
        /// The only instance, where this happens is when the pipeline plan is lowered to compiled query plan.
        /// Since we call this function beforehand, this should not be an issue.
        /// We add a check regardless, to ensure safety.
        if (const auto sharedPtrPredecessor = predecessor.lock())
        {
            predecessorIds.push_back(sharedPtrPredecessor->getPipelineId().getRawValue());
        }
        else
        {
            NES_ERROR("Pipeline predecessor was destroyed and can't be read from.")
        }
    }

    /// Serialize the contained operators as a seperate json.
    /// In a pipeline (as the name implies), each operator has mostly one successor, and mostly one predecessor.
    /// This makes displaying the parent-child relationships straight forward and does not require an additional getParents() function.
    nlohmann::json pipelineOperators;
    auto currentOp = pipeline->getRootOperator();
    /// Note that the root operator cannot have a parent operator in the same pipeline
    /// In general, the root is either a scan operator or a source operator
    std::vector<uint64_t> parentIds;
    auto child = currentOp.getChild();
    while (child)
    {
        uint64_t currentId = currentOp.getId().getRawValue();
        std::string currentLabel = currentOp.toString();
        std::vector<uint64_t> childIds = std::vector<uint64_t>{child->getId().getRawValue()};
        pipelineOperators.push_back({{"id", currentId}, {"label", currentLabel}, {"inputs", parentIds}, {"outputs", childIds}});
        parentIds = std::vector<uint64_t>{currentId};
        currentOp = child.value();
        child = currentOp.getChild();
    }
    /// Add entry for the last operator of the pipeline
    uint64_t currentId = currentOp.getId().getRawValue();
    std::string currentLabel = currentOp.toString();
    std::vector<uint64_t> childIds;
    pipelineOperators.push_back({{"id", currentId}, {"label", currentLabel}, {"inputs", parentIds}, {"outputs", childIds}});

    /// Create entry in the json of the plan
    resultJson->push_back(
        {{"pipeline_id", id}, {"operators", pipelineOperators}, {"successors", successorIds}, {"predecessors", predecessorIds}});

    /// Serialize all unseen successors
    for (const auto& successor : pipeline->getSuccessors())
    {
        uint64_t successorId = successor->getPipelineId().getRawValue();
        if (const auto foundId = std::ranges::find(visitedPipelines, successorId); foundId == visitedPipelines.end())
        {
            serializePipelineRecursive(resultJson, successor, visitedPipelines);
        }
    }

}

void PipelinedQueryPlan::serializeAsJson(nlohmann::json* resultJson) const
{
    /// Vector of already visited pipeline ids.
    /// We need this to keep track of visited pipelines so we don't serialize a pipeline twice
    std::vector<uint64_t> visitedPipelines;
    /// Iterate through all pipelines and serialize them.
    for (const auto& pipeline : pipelines)
    {
        serializePipelineRecursive(resultJson, pipeline, visitedPipelines);
    }
}


}
