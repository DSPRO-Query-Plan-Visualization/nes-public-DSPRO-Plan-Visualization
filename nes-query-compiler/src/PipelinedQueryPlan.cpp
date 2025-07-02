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
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SerializablePlanConbench.pb.h>

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
    std::vector<SerializablePipelineNode>& protoPipelines,
    const std::shared_ptr<Pipeline>& pipeline,
    std::vector<uint64_t>& visitedPipelines)
{
    /// Create the protobuf object for the current pipeline
    SerializablePipelineNode serializedPipeline;
    uint64_t id = pipeline->getPipelineId().getRawValue();
    serializedPipeline.set_pipeline_id(id);

    /// Add pipleline id to list of visited pipelines
    visitedPipelines.push_back(id);

    for (const auto& successor : pipeline->getSuccessors())
    {
        serializedPipeline.add_successors(successor->getPipelineId().getRawValue());
    }

    for (const auto& predecessor : pipeline->getPredecessors())
    {
        /// Since the predecessor might have been erased, we need to check if it still exists with lock.
        /// The only instance, where this happens is when the pipeline plan is lowered to compiled query plan.
        /// Since we call this function beforehand, this should not be an issue.
        /// We add a check regardless, to ensure safety.
        if (const auto sharedPtrPredecessor = predecessor.lock())
        {
            serializedPipeline.add_predecessors(sharedPtrPredecessor->getPipelineId().getRawValue());
        }
        else
        {
            NES_ERROR("Pipeline predecessor was destroyed and can't be read from.")
        }
    }

    /// Serialize the contained operators as a seperate protobuf message.
    /// In a pipeline (as the name implies), each operator has mostly one successor, and mostly one predecessor.
    /// This makes displaying the parent-child relationships straight forward and does not require an additional getParents() function.
    auto currentOp = pipeline->getRootOperator();
    /// Note that the root operator cannot have a parent operator in the same pipeline
    /// In general, the root is either a scan operator or a source operator
    std::vector<uint64_t> parentIds;
    auto child = currentOp.getChild();
    while (child)
    {
        uint64_t currentId = currentOp.getId().getRawValue();
        std::string currentLabel = currentOp.toString();
        uint64_t childId = child->getId().getRawValue();

        /// Set all attributes of the operators protobuf message and add the operator to the pipeline's operators
        SerializablePipelineNode_SerializablePhysicalOperatorNode* serializedOperator = serializedPipeline.add_operators();
        serializedOperator->set_id(currentId);
        serializedOperator->set_label(currentLabel);
        for (auto parentId : parentIds)
        {
            serializedOperator->add_inputs(parentId);
        }
        serializedOperator->add_outputs(childId);

        parentIds = std::vector{currentId};
        currentOp = child.value();
        child = currentOp.getChild();
    }
    /// Add last operator of the pipeline
    SerializablePipelineNode_SerializablePhysicalOperatorNode* serializedOperator = serializedPipeline.add_operators();
    uint64_t currentId = currentOp.getId().getRawValue();
    std::string currentLabel = currentOp.toString();

    /// Set values of last operator message (no outputs are added, it's the last operator of the pipeline)
    serializedOperator->set_id(currentId);
    serializedOperator->set_label(currentLabel);
    for (auto parentId : parentIds)
    {
        serializedOperator->add_inputs(parentId);
    }

    /// This pipeline message is fully created and can be pushed to the protoPipelines list
    protoPipelines.push_back(serializedPipeline);

    /// Serialize all unseen successors
    for (const auto& successor : pipeline->getSuccessors())
    {
        uint64_t successorId = successor->getPipelineId().getRawValue();
        if (const auto foundId = std::ranges::find(visitedPipelines, successorId); foundId == visitedPipelines.end())
        {
            serializePipelineRecursive(protoPipelines, successor, visitedPipelines);
        }
    }
}

void PipelinedQueryPlan::serializeAsJson(nlohmann::json* resultJson) const
{
    /// Vector of already visited pipeline ids.
    /// We need this to keep track of visited pipelines so we don't serialize a pipeline twice
    std::vector<uint64_t> visitedPipelines;

    /// We create a vector for pipeline protobuf serializations, which will afterwards be used do get the list of json pipeline representations
    std::vector<SerializablePipelineNode> protoPipelines = {};
    /// Iterate through all pipelines and serialize them.
    for (const auto& pipeline : pipelines)
    {
        serializePipelineRecursive(protoPipelines, pipeline, visitedPipelines);
    }

    /// Include empty lists in the json, like an empty inputs list for a source
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_fields_with_no_presence = true;

    /// Transform protobuf list to json list
    for (auto serializedPipeline : protoPipelines)
    {
        std::string pipeline_string;
        absl::Status status = google::protobuf::util::MessageToJsonString(serializedPipeline, &pipeline_string, options);
        const nlohmann::json pipelineJson = nlohmann::json::parse(pipeline_string);
        resultJson->push_back(pipelineJson);
    }
}


}
