from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Set, TypeVar

from dvc.fs import localfs
from dvc.utils.fs import path_isin

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.stage import Stage

T = TypeVar("T")


def check_acyclic(graph: "DiGraph") -> None:
    import networkx as nx

    from dvc.exceptions import CyclicGraphError

    try:
        edges = nx.find_cycle(graph, orientation="original")
    except nx.NetworkXNoCycle:
        return

    stages: Set["Stage"] = set()
    for from_node, to_node, _ in edges:
        stages.add(from_node)
        stages.add(to_node)

    raise CyclicGraphError(list(stages))


def get_pipeline(pipelines, node):
    found = [i for i in pipelines if i.has_node(node)]
    if not found:
        return None

    assert len(found) == 1
    return found[0]


def get_pipelines(graph: "DiGraph"):
    import networkx as nx

    return [graph.subgraph(c).copy() for c in nx.weakly_connected_components(graph)]


def get_subgraph_of_nodes(
    graph: "DiGraph", sources: Optional[List[Any]] = None, downstream: bool = False
) -> "DiGraph":
    from networkx import dfs_postorder_nodes, reverse_view

    if not sources:
        return graph

    g = reverse_view(graph) if downstream else graph
    nodes = []
    for source in sources:
        nodes.extend(dfs_postorder_nodes(g, source))
    return graph.subgraph(nodes)


def collect_pipeline(stage: "Stage", graph: "DiGraph") -> Iterator["Stage"]:
    import networkx as nx

    pipeline = get_pipeline(get_pipelines(graph), stage)
    if not pipeline:
        return iter([])

    return nx.dfs_postorder_nodes(pipeline, stage)


def collect_inside_path(path: str, graph: "DiGraph") -> List["Stage"]:
    import networkx as nx

    stages = nx.dfs_postorder_nodes(graph)
    return [stage for stage in stages if path_isin(stage.path, path)]


def build_graph(stages, outs_trie=None):
    """Generate a graph by using the given stages on the given directory

    The nodes of the graph are the stage's path relative to the root.

    Edges are created when the output of one stage is used as a
    dependency in other stage.

    The direction of the edges goes from the stage to its dependency:

    For example, running the following:

        $ dvc run -o A "echo A > A"
        $ dvc run -d A -o B "echo B > B"
        $ dvc run -d B -o C "echo C > C"

    Will create the following graph:

           ancestors <--
                       |
            C.dvc -> B.dvc -> A.dvc
            |          |
            |          --> descendants
            |
            ------- pipeline ------>
                       |
                       v
          (weakly connected components)

    Args:
        stages (list): used to build a graph from

    Raises:
        OutputDuplicationError: two outputs with the same path
        StagePathAsOutputError: stage inside an output directory
        OverlappingOutputPathsError: output inside output directory
        CyclicGraphError: resulting graph has cycles
    """
    import networkx as nx

    from dvc.exceptions import StagePathAsOutputError

    from .trie import build_outs_trie

    graph = nx.DiGraph()

    # Use trie to efficiently find overlapping outs and deps
    outs_trie = outs_trie or build_outs_trie(stages)

    for stage in stages:
        out = outs_trie.shortest_prefix(localfs.parts(stage.path)).value
        if out:
            raise StagePathAsOutputError(stage, str(out))

    # Building graph
    graph.add_nodes_from(stages)
    for stage in stages:
        if stage.is_repo_import:
            continue
        if stage.is_db_import:
            continue

        for dep in stage.deps:
            dep_key = dep.fs.parts(dep.fs_path)
            overlapping = [n.value for n in outs_trie.prefixes(dep_key)]
            if outs_trie.has_subtrie(dep_key):
                overlapping.extend(outs_trie.values(prefix=dep_key))

            graph.add_edges_from((stage, out.stage) for out in overlapping)
    check_acyclic(graph)

    return graph


# NOTE: using stage graph instead of just list of stages to make sure that it
# has already passed all the sanity checks like cycles/overlapping outputs and
# so on.
def build_outs_graph(graph, outs_trie):
    import networkx as nx

    outs_graph = nx.DiGraph()

    outs_graph.add_nodes_from(outs_trie.values())
    for stage in graph.nodes():
        if stage.is_repo_import:
            continue
        if stage.is_db_import:
            continue
        for dep in stage.deps:
            dep_key = dep.fs.parts(dep.fs_path)
            overlapping = [n.value for n in outs_trie.prefixes(dep_key)]
            if outs_trie.has_subtrie(dep_key):
                overlapping.extend(outs_trie.values(prefix=dep_key))

            for from_out in stage.outs:
                outs_graph.add_edges_from((from_out, out) for out in overlapping)
    return outs_graph
