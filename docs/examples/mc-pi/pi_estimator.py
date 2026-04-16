import random
import time
from pathlib import Path
from typing import override

import matplotlib.pyplot as plt
import numpy as np
import ray

from raygent import Task
from raygent.workflow import DAG

# Initialize ray if running standalone
if not ray.is_initialized():
    ray.init()


# --8<-- [start:viz_functions]
def quarter_circle_coords(resolution: int = 100) -> tuple[np.ndarray, np.ndarray]:
    """Compute the x,y coordinates of a quarter-circle of radius 1."""
    theta = np.linspace(0, np.pi / 2, resolution)
    return np.cos(theta), np.sin(theta)


def unit_square_coords() -> tuple[list[float], list[float]]:
    """Return the closed polygon (x,y) for the unit square."""
    xs = [0.0, 1.0, 1.0, 0.0, 0.0]
    ys = [0.0, 0.0, 1.0, 1.0, 0.0]
    return xs, ys


# --8<-- [end:viz_functions]


# --8<-- [start:plot_logic]
def save_monte_carlo_plot(filename: Path, n_samples: int = 500) -> None:
    """Generate and save a visual representation of the Monte Carlo method."""
    plt.figure(figsize=(5, 5))

    # Plot the bounding geometry
    xsq, ysq = unit_square_coords()
    plt.plot(xsq, ysq, lw=2, color="black", label="Unit Square")

    xc, yc = quarter_circle_coords(200)
    plt.plot(xc, yc, lw=2, color="blue", label="Quarter Circle (r=1)")

    # Generate random darts strictly for the visualization
    xs, ys = np.random.rand(n_samples), np.random.rand(n_samples)
    inside = (xs**2 + ys**2) <= 1
    outside = ~inside

    # Plot the darts, coloring them by success or failure
    plt.scatter(xs[inside], ys[inside], color="green", s=10, alpha=0.6, label="Inside")
    plt.scatter(xs[outside], ys[outside], color="red", s=10, alpha=0.6, label="Outside")

    plt.axis("equal")
    plt.xlim(-0.05, 1.05)
    plt.ylim(-0.05, 1.05)
    plt.xlabel("x")
    plt.ylabel("y")
    plt.legend(loc="center", fontsize="small")
    plt.title(f"Monte Carlo Pi Estimation ({n_samples} samples)")

    # Save the figure cleanly and free the memory
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()


# --8<-- [end:plot_logic]


# --8<-- [start:core_logic]
def sample_points(n_samples: int) -> int:
    """Generate uniformly random (x,y) in [0,1] x [0,1]."""
    n_inside = 0
    for _ in range(n_samples):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1:
            n_inside += 1
    return n_inside


def estimate_pi(n_samples: int, n_inside: int) -> float:
    """Calculate the final Pi estimate."""
    return 4.0 * (n_inside / n_samples)


# --8<-- [end:core_logic]


# --8<-- [start:raygent_task]
class MonteCarloPiTask(Task):
    @override
    def do(self, batch: list[int], *args: object, **kwargs: object) -> int:
        """Simulates a Monte Carlo experiment for estimating Pi.

        The DAG engine passes data in batches. We process each sample count
        in the batch and return the corresponding inside-counts.
        """
        return sample_points(batch[0])


# --8<-- [end:raygent_task]


# --8<-- [start:raygent_execution]
def estimate_pi_parallel(n_samples: int, n_workers: int) -> tuple[float, float]:
    """Estimates Pi using Monte Carlo simulation with parallel DAG execution."""

    samples_per_worker = n_samples // n_workers
    workload_stream = [samples_per_worker] * n_workers

    # Build the DAG Blueprint
    dag = DAG()

    source_handle, source_queue = dag.add_source(name="sample_injector")

    mc_node = dag.add(
        MonteCarloPiTask(),
        inputs=source_handle,
        n_workers=n_workers,
        name="mc_worker_pool",
    )

    sink_queue = dag.add_sink(mc_node)

    # Execute the Graph
    t_start = time.time()

    dag.start()

    total_inside = 0

    # The stream generator pushes the workload into the source and yields from the sink
    for _, message in dag.stream(
        workload_stream,
        source_queues=[source_queue],
        sink_queues=[sink_queue],
        batch_size=1,
    ):
        # message.payload contains the results returned by `do`
        total_inside += sum(message.payload)

    dag.stop()

    t_stop = time.time()

    pi_estimate = estimate_pi(n_samples, total_inside)

    return pi_estimate, t_stop - t_start


# --8<-- [end:raygent_execution]


if __name__ == "__main__":
    # Create an output directory for our documentation assets
    output_dir = Path(__file__).resolve().parent

    print("Generating educational figures...")
    viz_path = output_dir / "monte_carlo_viz.png"
    save_monte_carlo_plot(viz_path, n_samples=1000)
    print(f"Visualization saved to {viz_path.resolve()}\n")

    # You can now run this script directly to test your logic.
    n_samples = 1_000
    n_workers = 8

    print(
        f"Running parallel estimation with {n_samples} samples across {n_workers} workers..."
    )
    pi_est, t_delta = estimate_pi_parallel(n_samples, n_workers)

    print(f"Pi     ≈ {pi_est:.8f}")
    print(f"Error  = {np.pi - pi_est:.8f}")
    print(f"Time   = {t_delta:.4f} s")
