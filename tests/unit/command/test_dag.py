import networkx as nx
import pytest

from dvc.cli import parse_args
from dvc.command.dag import CmdDAG, _build, _show_ascii, _show_dot


@pytest.mark.parametrize("fmt", [None, "--dot"])
def test_dag(tmp_dir, dvc, mocker, fmt):
    tmp_dir.dvc_gen("foo", "foo")

    args = ["dag", "--full", "foo.dvc"]
    if fmt:
        args.append(fmt)
    cli_args = parse_args(args)
    assert cli_args.func == CmdDAG

    cmd = cli_args.func(cli_args)

    mocker.patch("dvc.command.dag._build", return_value=dvc.graph)

    assert cmd.run() == 0


@pytest.fixture
def graph(tmp_dir, dvc):
    tmp_dir.dvc_gen("a", "a")
    tmp_dir.dvc_gen("b", "b")

    dvc.run(
        no_exec=True, deps=["a", "c"], outs=["d", "e"], cmd="cmd1", name="1"
    )
    dvc.run(
        no_exec=True, deps=["b", "c"], outs=["f", "g"], cmd="cmd2", name="2"
    )
    dvc.run(
        no_exec=True,
        deps=["a", "b", "c"],
        outs=["h", "i"],
        cmd="cmd3",
        name="3",
    )
    dvc.run(no_exec=True, deps=["a", "h"], outs=["j"], cmd="cmd4", name="4")

    return dvc.graph


def test_build(graph):
    assert nx.is_isomorphic(_build(graph), graph)


def test_build_target(graph):
    (stage,) = filter(
        lambda s: hasattr(s, "name") and s.name == "3", graph.nodes()
    )
    G = _build(graph, target=stage)
    assert set(G.nodes()) == {"3", "b.dvc", "a.dvc"}
    assert set(G.edges()) == {("3", "a.dvc"), ("3", "b.dvc")}


def test_build_target_with_outs(graph):
    (stage,) = filter(
        lambda s: hasattr(s, "name") and s.name == "3", graph.nodes()
    )
    G = _build(graph, target=stage, outs=True)
    assert set(G.nodes()) == {"a", "b", "h", "i"}
    assert set(G.edges()) == {
        ("h", "a"),
        ("h", "b"),
        ("i", "a"),
        ("i", "b"),
    }


def test_build_full(graph):
    (stage,) = filter(
        lambda s: hasattr(s, "name") and s.name == "3", graph.nodes()
    )
    G = _build(graph, target=stage, full=True)
    assert nx.is_isomorphic(G, graph)


def test_show_ascii(graph):
    assert [line.rstrip() for line in _show_ascii(graph).splitlines()] == [
        "                        +----------------+                          +----------------+",  # noqa: E501
        "                        | stage: 'a.dvc' |                          | stage: 'b.dvc' |",  # noqa: E501
        "                       *+----------------+****                      +----------------+",  # noqa: E501
        "                  *****           *           *****                  ***           ***",  # noqa: E501
        "              ****                *                *****           **                 **",  # noqa: E501
        "           ***                     *                    ***      **                     **",  # noqa: E501
        "+------------+                     **                   +------------+              +------------+",  # noqa: E501
        "| stage: '1' |                       **                 | stage: '3' |              | stage: '2' |",  # noqa: E501
        "+------------+                         ***              +------------+              +------------+",  # noqa: E501
        "                                          **           ***",
        "                                            **       **",
        "                                              **   **",
        "                                          +------------+",
        "                                          | stage: '4' |",
        "                                          +------------+",
    ]


def test_show_dot(graph):
    assert _show_dot(graph) == (
        "strict digraph  {\n"
        "stage;\n"
        "stage;\n"
        "stage;\n"
        "stage;\n"
        "stage;\n"
        "stage;\n"
        "\"stage: '1'\" -> \"stage: 'a.dvc'\";\n"
        "\"stage: '2'\" -> \"stage: 'b.dvc'\";\n"
        "\"stage: '3'\" -> \"stage: 'a.dvc'\";\n"
        "\"stage: '3'\" -> \"stage: 'b.dvc'\";\n"
        "\"stage: '4'\" -> \"stage: 'a.dvc'\";\n"
        "\"stage: '4'\" -> \"stage: '3'\";\n"
        "}\n"
    )
