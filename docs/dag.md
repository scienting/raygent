# DAGs

**`raygent`** simplifies parallel execution in Python by wrapping Ray's distributed computing framework.
An individual [`Task`][raygent.task.Task] handles an isolated computation on a batch of data.
However, real-world processing pipelines usually require multiple sequential steps.

The Directed Acyclic Graph (DAG) orchestrates these multi-step workflows.
The [`DAG`][raygent.workflow.dag.DAG] class lets you wire parallelized computational tasks (nodes) together.
You can also connect them with data transformations (edges).
Its main role is to provide a modular framework for managing parallel tasks.
This makes it easy to scale a deterministic, batch-synchronous pipeline across multiple cores and nodes.

Think of a DAG as a factory assembly line.
Raw materials (data) enter at the start, move along conveyor belts (queues), and reach workstations (tasks) where workers (Ray actors) transform them.
The graph is directional.
Data only moves forward.

## Building the DAG

The [`DAG`][raygent.workflow.dag.DAG] object is a stateful builder for a deterministic, queue-backed execution graph.
As you hand it tasks and connections, it actively wires the necessary Ray actors and [`BoundedQueue`][raygent.workflow.queue.BoundedQueue] edges in the background.

Because data moves in one direction, you must build the pipeline strictly downstream.
You cannot connect a node until all of its inputs already exist.
This rule forces a logical progression from source to sink and naturally prevents infinite loops or cycles.

### Sources: The Entry Points

Every pipeline needs a loading dock.
You create one by calling [`add_source`][raygent.workflow.dag.DAG.add_source].
This method requires no upstream inputs and returns a tuple containing two distinct objects: a [`NodeHandle`][raygent.workflow.node.NodeHandle] and a [`BoundedQueue`][raygent.workflow.queue.BoundedQueue].

These two objects serve different purposes.
The [`BoundedQueue`][raygent.workflow.queue.BoundedQueue] is your funnel; later, when the graph is running, you will push your raw data iterables into this queue.
The [`NodeHandle`][raygent.workflow.node.NodeHandle] is your structural anchor; you pass this handle to downstream nodes so they know exactly where to pull their data from.

```python
# Create a source. We will send raw text into text_queue later.
# We pass text_handle to the first processing node.
text_handle, text_queue = dag.add_source(name="raw_text")
```

### Nodes: The Processing Centers

Nodes perform the actual labor.
To insert a computational step, you call [`add`][raygent.workflow.dag.DAG.add].
This method requires an instantiated [`Task`][raygent.task.Task] and one or more upstream [`NodeHandle`][raygent.workflow.node.NodeHandle] objects to serve as inputs.
If your task accepts multiple positional arguments, the order you list the inputs determines which argument receives which data.

This is also where you allocate hardware.
By setting [`n_workers`][raygent.workflow.dag.DAG.add(n_workers)], you dictate how many parallel Ray actors run this specific task concurrently.
By setting [`n_cpu_per_worker`][raygent.workflow.dag.DAG.add(n_cpu_per_worker)], you define how many logical cores each individual actor controls.
The builder tracks these requests.
If your pipeline demands more cores than the DAG's configured [`max_cores`][raygent.workflow.dag.DAG.max_cores], it will log a warning.

```python
# Connect a task to the source.
# We give this heavy task 4 parallel workers, each using 1 core.
clean_node = dag.add(
    CleanTextTask(),
    inputs=text_handle,
    n_workers=4,
    n_cpu_per_worker=1,
    name="cleaner"
)
```

### Sinks: The Exit Points

Sinks serve as the extraction points.
By passing a [`NodeHandle`][raygent.workflow.node.NodeHandle] into [`add_sink`][raygent.workflow.dag.DAG.add_sink], you create an exit valve.
The method returns a [`BoundedQueue`][raygent.workflow.queue.BoundedQueue].
When the graph runs, you pull your final, processed batch messages out of this queue.

However, sinks are not free.
Behind the scenes, [`add_sink`][raygent.workflow.dag.DAG.add_sink] instantiates an identity task and an actor to manage the outbound flow.
Adding a single sink consumes computational resources.
Attaching too many sinks to intermediate steps will quietly cannibalize your system's resources and reduce your overall parallelization.
Only place sinks where you strictly need to harvest data.

```python
# Attach a sink to the final processing node to collect the results.
results_queue = dag.add_sink(clean_node, name="final_output")
```

## Execution

Defining a [`Task`][raygent.task.Task] graph only establishes the structure of your computation.
To process data, you must transition from this static definition to an active distributed system.
The framework manages this execution phase through three distinct operational steps: resource allocation, data ingestion, and resource deallocation.

### Resource Allocation

The [`start()`][raygent.workflow.dag.DAG.start] method initializes the distributed infrastructure.
The framework allocates the required CPU and GPU resources across the Ray cluster and deploys active worker processes (actors) for every node in your graph.
These workers enter a continuous event loop, listening to their assigned input queues and waiting for data.

### Data Ingestion and Backpressure

The [`stream()`][raygent.workflow.dag.DAG.stream] generator manages the movement of data through the initialized graph.
It accepts raw iterables, chunks them into optimized batches, and pushes them into the source queues.

Crucially, [`stream()`][raygent.workflow.dag.DAG.stream] manages system memory by enforcing backpressure.
Using the `max_inflight` parameter, it continuously monitors the cluster's processing rate.
If downstream Tasks operate slower than the ingestion rate, [`stream()`][raygent.workflow.dag.DAG.stream] automatically pauses data intake to prevent memory exhaustion.
As data successfully propagates through the graph, the generator yields the final outputs from the sink queues.

### Termination

The [`stop()`][raygent.workflow.dag.DAG.stop] method terminates the execution lifecycle.
Because the [`start()`][raygent.workflow.dag.DAG.start] method places workers into continuous event loops, they will not automatically shut down when the input data is exhausted.
The [`stop()`][raygent.workflow.dag.DAG.stop] method explicitly breaks these loops, terminates the worker processes, and releases the allocated hardware back to the Ray cluster.
Failing to execute this step causes resource leaks by leaving idle processes running.

## Putting It All Together

Up to this point, we have examined the individual components of the framework in isolation.
We have built Tasks and discussed the mechanics of execution.
Now, we must assemble them.
Real-world applications rarely consist of a single mathematical operation.
They require complex, multi-stage pipelines where data diverges, undergoes heavy processing, and converges back together.

Let us construct a realistic system.
Imagine we are ingesting a continuous stream of user feedback.
We receive raw user IDs from one source and the corresponding unformatted text comments from another.
Our goal is to sanitize the text, append an internal tracking tag, and seamlessly merge the cleaned text back with its original user ID.

### Mapping the Architecture

Before writing any implementation logic, we must map the high-level architecture of the graph.
We cannot build an efficient system by blindly connecting functions.
We need a deliberate structural blueprint.

Our pipeline requires two distinct entry points.
The user IDs will flow down a direct, unhindered path.
The text comments, however, must pass through a two-stage processing branch.
First, a cleaning node will normalize the strings.
Because text parsing is often computationally heavy, we will allocate two dedicated workers to this specific node to prevent a bottleneck.
Second, a tagging node will inject our configuration data.

Finally, these two paths will converge at a merging node.
This node will synchronize the incoming batches from both the fast lane (the IDs) and the processing lane (the text), constructing the final unified output.

### The Implementation

With the high-level topology established, we translate our blueprint into active code.
We define our isolated Tasks, wire the Directed Acyclic Graph (DAG), and stream the data.

```python
import ray
from raygent import Task
from raygent.workflow import DAG


###   Define the Computational Units   ###

class CleanTextTask(Task):
    def do(self, texts: list[str]) -> list[str]:
        return [t.strip().lower() for t in texts]

class TagTask(Task):
    # 'tag' is exposed as a keyword argument for dynamic configuration.
    def do(self, texts: list[str], tag: str = "general") -> list[str]:
        return [f"[{tag}] {t}" for t in texts]

class MergeRecordTask(Task):
    # This Task expects two synchronized positional arguments.
    def do(self, ids: list[int], texts: list[str]) -> list[dict]:
        return [{"user_id": i, "content": t} for i, t in zip(ids, texts)]


###   Wire the High-Level Graph   ###

ray.init()
dag = DAG()

id_handle, id_source_queue = dag.add_source(name="id_source")
text_handle, text_source_queue = dag.add_source(name="text_source")

clean_node = dag.add(
    CleanTextTask(), 
    inputs=text_handle,
    n_workers=2, # Allocate extra hardware to the heavy step
    name="cleaner"
)

# Continue the branch, passing configuration data via kwargs
tag_node = dag.add(
    TagTask(),
    inputs=clean_node,
    task_kwargs={"tag": "processed"},
    name="tagger"
)

# Converge the streams
# The order of the inputs tuple strictly dictates the order of arguments in 'do'
merge_node = dag.add(
    MergeRecordTask(),
    inputs=(id_handle, tag_node), 
    name="merger"
)

# Establish the exit point
final_sink_queue = dag.add_sink(merge_node)


###   Execute the Pipeline   ###

raw_ids = [101, 102, 103]
raw_texts = ["  HELLO World  ", "Great Product! ", "   needs work."]

dag.start()

# Safely push multiple iterables into their respective source queues
for sink_idx, message in dag.stream(
    raw_ids,
    raw_texts,
    source_queues=(id_source_queue, text_source_queue),
    sink_queues=[final_sink_queue],
    batch_size=2,
):
    print(f"Batch {message.index} output: {message.payload}")

dag.stop()
```

### Analyzing the Execution

Notice how the structural rules we discussed earlier manifest directly in this code.

The `TagTask` demonstrates the dynamic configuration pattern.
We pass `"processed"` as a dictionary via `task_kwargs` during the DAG wiring.
This permanently sets the behavior of that specific node without hardcoding the string into the Task's internal logic.

Crucially, observe the `MergeRecordTask`.
Its `do` method expects `ids` first, and `texts` second.
Therefore, when we wire the `merge_node`, we must pass the upstream handles as a tuple in that exact order: `inputs=(id_handle, tag_node)`.
The physical wiring strictly dictates the argument delivery to the node.
If we reverse the tuple, the framework will attempt to pass text strings into the integer argument, crashing the pipeline.

The `stream` generator safely orchestrates the dual intake.
It accepts our two independent raw lists and our two designated source queues.
It handles the complexity of synchronizing the batch creation and ingestion process across the distributed cluster, ensuring the user IDs and text comments remain perfectly aligned as they traverse the graph.
