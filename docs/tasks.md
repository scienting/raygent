# Tasks

We generate more data today than a single computer can process.
When you need to analyze a million images, simulate years of climate data, or parse a massive database, you cannot rely on a single processor.
You must distribute the work across many machines.
However, writing the code to coordinate multiple processors is notoriously difficult.
It introduces race conditions, deadlocks, and complex boilerplate that distracts you from your actual goal.

The **`raygent`** framework solves this scaling problem through the concept of a [`Task`][raygent.task.Task].
A [`Task`][raygent.task.Task] represents an isolated computational step.
By designing your logic as self-contained tasks, you allow **`raygent`** to automatically scale your processing pipeline across multiple machines without writing any complex parallel programming.

## The `do` Method

Every [`Task`][raygent.task.Task] requires you to implement a specific method called [`do`][raygent.task.Task.do].
This method serves as the engine of your [`Task`][raygent.task.Task]; it contains the actual computational logic that executes your work.

To use this engine effectively, we must understand how data flows into it.
Standard data processing tools often feed data into a function one piece at a time.
This approach severely limits performance at scale.

!!! quote "Conceptual Example"

    Imagine you need to classify text documents using a massive artificial intelligence model.
    If your function receives one sentence at a time, it must load the heavy model into memory, classify the text, and then discard the model.
    Repeating this expensive setup process for a million individual sentences wastes enormous amounts of computational time.

The [`do`][raygent.task.Task.do] method avoids this bottleneck by receiving data in batches.
A batch is a collection of data points processed simultaneously, such as a list of strings or a NumPy array of numbers.
By receiving a batch, you perform expensive setup operations exactly once.

!!! example

    Let us build a simple [`Task`][raygent.task.Task] to see batch processing in practice.
    Suppose we have a large dataset of measurements stored in a two-dimensional array.
    Each row represents a different sensor, and each column represents a reading taken over time.
    We want to calculate the average reading for each sensor.

    We begin by defining a class that inherits from [`Task`][raygent.task.Task].
    Inside this class, we implement the [`do`][raygent.task.Task.do] method to accept our batch of array data.

    ```python
    import numpy as np
    import numpy.typing as npt
    from raygent import Task


    class MeanTask(Task):
        def do(self, batch: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
            """
            We calculate the mean across the columns (axis=1) for the entire batch.
            Vectorized operations process the batch instantly without looping.
            """
            return np.mean(batch, axis=1)
    ```

    Because **`raygent`** provides the entire batch at once, we do not need to write an inefficient loop to process one sensor at a time.
    We simply apply our vectorized mathematical operation across the entire batch and return the combined results.
    We can simulate a batch of data: two sensors, three readings each.

    ```python
    data = np.array([[88.0, 92.0, 85.0], [70.0, 75.0, 80.0]], dtype=np.float64)
    task = MeanTask()
    result = task.do(data)
    print(result)  # [88.33333333 75.        ]
    ```

### The Rules of Task Design

To harness the full power of a distributed system, we must enforce strict boundaries on how our code operates.
If a single computational unit behaves unpredictably, the entire cluster suffers.
Therefore, designing a [`Task`][raygent.task.Task] in **`raygent`** requires adhering to a specific set of operational principles.

Rather than viewing these principles as arbitrary rules, we should understand them as the structural requirements that allow the framework to parallelize your work safely and efficiently.

### Structuring Task Inputs: Data vs. Configuration

When a [`Task`][raygent.task.Task] executes on a remote machine, the framework needs to know exactly what information to slice up and distribute, and what information to copy perfectly to every worker.
We achieve this critical separation through how we define the arguments in our [`do`][raygent.task.Task.do] method.

You must pass your core data batches strictly as positional arguments.
Conversely, you must pass any configuration parameters (such as a scaling factor, a file path, or an API key) as keyword arguments.

If you blur this line, the framework fails.
If you pass a file path as a positional argument, the system might assume it is batch data and attempt to split the string character by character across your cluster.
By strictly using keyword arguments for configuration, you tell the engine, "Copy this exact setting to every machine running this Task."

!!! example

    Suppose we want to normalize a large array of sensor readings.
    The readings represent the data batch.
    The normalization scale is a configuration parameter.
    We separate them in the function signature.

    ```python
    import numpy as np
    import numpy.typing as npt
    from raygent import Task


    class NormalizeTask(Task):
        def do(self, batch: npt.NDArray[np.float64], scale_factor: float = 1.0) -> npt.NDArray[np.float64]:
            return batch * scale_factor
    ```

### Maintaining Strict Isolation

A **`raygent`** [`Task`][raygent.task.Task] must act as a sovereign entity.
It performs its specific job and returns a result.
It absolutely cannot invoke another Task from within its [`do`][raygent.task.Task.do] method.

To understand why, consider how the framework allocates resources.
When a [`Task`][raygent.task.Task] begins, it claims a CPU core.
If Task A calls Task B, Task A stops working and waits for Task B to finish.
However, Task A does not release its CPU core.
It holds the core hostage while doing nothing.
In a massive pipeline, this nesting leads to deadlocks where all cores are waiting on other Tasks to finish, bringing your computation to a grinding halt.

If your workflow requires sequential steps, you must separate them.
Do not build Russian dolls where one Task hides inside another.
Instead, build an assembly line.
Create `TaskA` and a subsequent `TaskB`.
You then connect them using **`raygent`**'s [Directed Acyclic Graph (DAG)](./dag.md) system, which orchestrates the handoff between them efficiently.

### Resource Transparency

The **`raygent`** engine meticulously manages hardware.
It assigns specific CPU cores to specific Tasks to prevent system overloads.
However, the engine can only manage what it can see.
It assumes your [`do`][raygent.task.Task.do] method executes in a single thread unless told otherwise.

If you write a [`do`][raygent.task.Task.do] method that secretly spins up its own internal threads, you blind the resource manager.

!!! quote "Conceptual Example"

    Imagine assigning one worker to a small room, only for that worker to sneak ten friends through the window.
    The room becomes instantly overcrowded, and nobody can work efficiently.

This often happens inadvertently when using underlying C-libraries or machine learning frameworks that default to using all available cores on a machine.

If your [`Task`][raygent.task.Task] relies on internal multithreading, you must explicitly declare this behavior in the class docstring. This transparency is vital.
It alerts the user that they must manually configure the framework to allocate a larger share of the machine's resources to this specific [`Task`][raygent.task.Task], preventing throttling across the cluster.

## Design Patterns in Practice

When you build complex data pipelines, you will encounter the same structural challenges repeatedly.
How do you handle multiple inputs?
Where do you put expensive setup operations?

The [`do`][raygent.task.Task.do] method provides a flexible canvas to solve these problems.
By recognizing and applying a few core design patterns, you can write Tasks that are highly performant, easily reusable, and perfectly aligned with the framework's distributed architecture.

### Pattern 1: Hardware-Level Vectorization

Standard Python loops are notoriously slow for mathematical operations.
If you attempt to iterate through a massive batch of numerical data item by item within your [`do`][raygent.task.Task.do] method, you will instantly create a performance bottleneck.

To achieve maximum throughput, you must bypass Python's looping mechanism entirely.
You achieve this by passing the entire batch directly into optimized libraries like NumPy or Polars. These libraries push the iteration down to the hardware level, executing the mathematics in highly optimized C or Rust backends.

Suppose you are building a library to process molecular data and need to calculate the center of mass for thousands of atomic structures.
Rather than looping through each structure, you feed the entire matrix of coordinates into a vectorized function.
The computation happens almost instantaneously across the entire batch.

```python
import numpy as np
import numpy.typing as npt
from raygent import Task


class MeanTask(Task):

    def do(self, batch: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.mean(batch, axis=1)
```

### Pattern 2: Converging Data Streams

Real-world computation rarely relies on a single dataset.
You will frequently need to combine different streams of information, such as merging real-time sensor readings with historical baseline data.

In a distributed pipeline, you connect these different data sources to your [`Task`][raygent.task.Task] using a [Directed Acyclic Graph (DAG)](./dag.md).
When a [`Task`][raygent.task.Task] sits at the junction of two upstream sources, the framework synchronizes the data delivery.
The [`do`][raygent.task.Task.do] method receives these synchronized batches simultaneously.
You simply declare a positional argument for every incoming stream, ensuring the order of your arguments matches the physical wiring of your pipeline.

```python
from raygent import Task


class SumTask(Task):
    def do(self, batch_a: list[int], batch_b: list[int]) -> list[int]:
        return [a + b for a, b in zip(batch_a, batch_b)]
```

### Pattern 3: Dynamic Configuration

Hardcoding values inside your computational logic makes your code brittle.
If you write a [`Task`][raygent.task.Task] that multiplies data by a factor of two, and later need to multiply data by a factor of ten, you should not have to write a completely new class.

You must design your Tasks to be reusable.
By exposing internal variables as keyword arguments in the [`do`][raygent.task.Task.do] method, you create a control panel for your [`Task`][raygent.task.Task].
This pattern allows you to deploy the exact same underlying logic across different parts of your pipeline, dynamically altering its behavior by passing different configuration settings from the outside.

```python
from raygent import Task


class PrefactorTask(Task):

    def do(self, batch: list[int], factor: int = 1) -> list[int]:
        return [factor * x for x in batch]
```

### Pattern 4: The Shared Setup Bottleneck

In modern data processing, the actual computation is rarely the most expensive step.
The true cost lies in the setup: establishing secure network connections, allocating memory, or moving massive files from disk into RAM.

Consider a [`Task`][raygent.task.Task] designed to convert text documents into numerical vectors using a large language model.
If the framework forced you to process one document at a time, your code would have to load the multi-gigabyte model into memory, process a single sentence, and then discard the model—millions of times over.
Your pipeline would paralyze the machine.

Because the [`do`][raygent.task.Task.do] method receives a batch, you gain control over the setup lifecycle.
You pay the heavy initialization cost exactly once at the top of the method.
You then process thousands of items using that shared resource before the method concludes.

```python
from typing import Any
from raygent import Task


class DocumentVectorizerTask(Task):
    def do(self, batch: list[str], **kwargs: Any) -> list[dict[str, Any]]:
        vectorizer = load_large_language_model()
        embeddings = vectorizer.encode_batch(batch)
        return [
            {"document": doc, "vector": vec}
            for doc, vec in zip(batch, embeddings)
        ]
```
