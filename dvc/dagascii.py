"""Draws DAG in ASCII."""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import math

from grandalf.graphs import Vertex, Edge, Graph
from grandalf.layouts import SugiyamaLayout
from grandalf.routing import route_with_lines, EdgeViewer


class VertexViewer(object):
    """Class to define vertex box boundaries that will be accounted for during
    graph building by grandalf.

    Args:
        name (str): name of the vertex.
    """

    HEIGHT = 3  # top and bottom box edges + text

    def __init__(self, name):
        # pylint: disable=invalid-name
        self._h = self.HEIGHT  # top and bottom box edges + text
        self._w = len(name) + 2  # right and left bottom edges + text

    @property
    def h(self):  # pylint: disable=invalid-name
        """Height of the box."""
        return self._h

    @property
    def w(self):  # pylint: disable=invalid-name
        """Width of the box."""
        return self._w


class AsciiCanvas(object):
    """Class for drawing in ASCII.

    Args:
        cols (int): number of columns in the canvas. Should be > 1.
        lines (int): number of lines in the canvas. Should be > 1.
    """

    TIMEOUT = 10

    def __init__(self, cols, lines):
        assert cols > 1
        assert lines > 1

        self.cols = cols
        self.lines = lines

        self.canvas = [[" "] * cols for l in range(lines)]

    def draw(self):
        """Draws ASCII canvas on the screen."""
        if sys.stdout.isatty():  # pragma: no cover
            from asciimatics.screen import Screen

            Screen.wrapper(self._do_draw)
        else:
            for line in self.canvas:
                print("".join(line))

    def _do_draw(self, screen):  # pragma: no cover
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches, too-many-statements
        from dvc.system import System
        from asciimatics.event import KeyboardEvent

        offset_x = 0
        offset_y = 0
        smaxrow, smaxcol = screen.dimensions
        assert smaxrow > 1
        assert smaxcol > 1
        smaxrow -= 1
        smaxcol -= 1

        if self.lines + 1 > smaxrow:
            max_y = self.lines + 1 - smaxrow
        else:
            max_y = 0

        if self.cols + 1 > smaxcol:
            max_x = self.cols + 1 - smaxcol
        else:
            max_x = 0

        while True:
            for y in range(smaxrow + 1):
                y_index = offset_y + y
                line = []
                for x in range(smaxcol + 1):
                    x_index = offset_x + x
                    if (
                        len(self.canvas) > y_index
                        and len(self.canvas[y_index]) > x_index
                    ):
                        line.append(self.canvas[y_index][x_index])
                    else:
                        line.append(" ")
                assert len(line) == (smaxcol + 1)
                screen.print_at("".join(line), 0, y)

            screen.refresh()

            # NOTE: get_event() doesn't block by itself,
            # so we have to do the blocking ourselves.
            #
            # NOTE: using this workaround while waiting for PR [1]
            # to get merged and released. After that need to adjust
            # asciimatics version requirements.
            #
            # [1] https://github.com/peterbrittain/asciimatics/pull/188
            System.wait_for_input(self.TIMEOUT)

            event = screen.get_event()
            if not isinstance(event, KeyboardEvent):
                continue

            k = event.key_code
            if k == screen.KEY_DOWN or k == ord("s"):
                offset_y += 1
            elif k == screen.KEY_PAGE_DOWN or k == ord("S"):
                offset_y += smaxrow
            elif k == screen.KEY_UP or k == ord("w"):
                offset_y -= 1
            elif k == screen.KEY_PAGE_UP or k == ord("W"):
                offset_y -= smaxrow
            elif k == screen.KEY_RIGHT or k == ord("d"):
                offset_x += 1
            elif k == ord("D"):
                offset_x += smaxcol
            elif k == screen.KEY_LEFT or k == ord("a"):
                offset_x -= 1
            elif k == ord("A"):
                offset_x -= smaxcol
            elif k == ord("q") or k == ord("Q"):
                break

            if offset_y > max_y:
                offset_y = max_y
            elif offset_y < 0:
                offset_y = 0

            if offset_x > max_x:
                offset_x = max_x
            elif offset_x < 0:
                offset_x = 0

    def point(self, x, y, char):
        """Create a point on ASCII canvas.

        Args:
            x (int): x coordinate. Should be >= 0 and < number of columns in
                the canvas.
            y (int): y coordinate. Should be >= 0 an < number of lines in the
                canvas.
            char (str): character to place in the specified point on the
                canvas.
        """
        assert len(char) == 1
        assert x >= 0
        assert x < self.cols
        assert y >= 0
        assert y < self.lines

        self.canvas[y][x] = char

    def line(self, x0, y0, x1, y1, char):
        """Create a line on ASCII canvas.

        Args:
            x0 (int): x coordinate where the line should start.
            y0 (int): y coordinate where the line should start.
            x1 (int): x coordinate where the line should end.
            y1 (int): y coordinate where the line should end.
            char (str): character to draw the line with.
        """
        # pylint: disable=too-many-arguments, too-many-branches
        if x0 > x1:
            x1, x0 = x0, x1
            y1, y0 = y0, y1

        dx = x1 - x0
        dy = y1 - y0

        if dx == 0 and dy == 0:
            self.point(x0, y0, char)
        elif abs(dx) >= abs(dy):
            for x in range(x0, x1 + 1):
                if dx == 0:
                    y = y0
                else:
                    y = y0 + int(round((x - x0) * dy / float((dx))))
                self.point(x, y, char)
        elif y0 < y1:
            for y in range(y0, y1 + 1):
                if dy == 0:
                    x = x0
                else:
                    x = x0 + int(round((y - y0) * dx / float((dy))))
                self.point(x, y, char)
        else:
            for y in range(y1, y0 + 1):
                if dy == 0:
                    x = x0
                else:
                    x = x1 + int(round((y - y1) * dx / float((dy))))
                self.point(x, y, char)

    def text(self, x, y, text):
        """Print a text on ASCII canvas.

        Args:
            x (int): x coordinate where the text should start.
            y (int): y coordinate where the text should start.
            text (str): string that should be printed.
        """
        for i, char in enumerate(text):
            self.point(x + i, y, char)

    def box(self, x0, y0, width, height):
        """Create a box on ASCII canvas.

        Args:
            x0 (int): x coordinate of the box corner.
            y0 (int): y coordinate of the box corner.
            width (int): box width.
            height (int): box height.
        """
        assert width > 1
        assert height > 1

        width -= 1
        height -= 1

        for x in range(x0, x0 + width):
            self.point(x, y0, "-")
            self.point(x, y0 + height, "-")

        for y in range(y0, y0 + height):
            self.point(x0, y, "|")
            self.point(x0 + width, y, "|")

        self.point(x0, y0, "+")
        self.point(x0 + width, y0, "+")
        self.point(x0, y0 + height, "+")
        self.point(x0 + width, y0 + height, "+")


def _build_sugiyama_layout(vertexes, edges):
    #
    # Just a reminder about naming conventions:
    # +------------X
    # |
    # |
    # |
    # |
    # Y
    #

    vertexes = {v: Vertex(" {} ".format(v)) for v in vertexes}
    # NOTE: reverting edges to correctly orientate the graph
    edges = [Edge(vertexes[e], vertexes[s]) for s, e in edges]
    vertexes = vertexes.values()
    graph = Graph(vertexes, edges)

    for vertex in vertexes:
        vertex.view = VertexViewer(vertex.data)

    # NOTE: determine min box length to create the best layout
    minw = min([v.view.w for v in vertexes])

    for edge in edges:
        edge.view = EdgeViewer()

    sug = SugiyamaLayout(graph.C[0])
    graph = graph.C[0]
    roots = list(filter(lambda x: len(x.e_in()) == 0, graph.sV))

    sug.init_all(roots=roots, optimize=True)

    sug.yspace = VertexViewer.HEIGHT
    sug.xspace = minw
    sug.route_edge = route_with_lines

    sug.draw()

    return sug


def draw(vertexes, edges):
    """Build a DAG and draw it in ASCII.

    Args:
        vertexes (list): list of graph vertexes.
        edges (list): list of graph edges.
    """
    # pylint: disable=too-many-locals
    # NOTE: coordinates might me negative, so we need to shift
    # everything to the positive plane before we actually draw it.
    Xs = []  # pylint: disable=invalid-name
    Ys = []  # pylint: disable=invalid-name

    sug = _build_sugiyama_layout(vertexes, edges)

    for vertex in sug.g.sV:
        # NOTE: moving boxes w/2 to the left
        Xs.append(vertex.view.xy[0] - vertex.view.w / 2.0)
        Xs.append(vertex.view.xy[0] + vertex.view.w / 2.0)
        Ys.append(vertex.view.xy[1])
        Ys.append(vertex.view.xy[1] + vertex.view.h)

    for edge in sug.g.sE:
        for x, y in edge.view._pts:  # pylint: disable=protected-access
            Xs.append(x)
            Ys.append(y)

    minx = min(Xs)
    miny = min(Ys)
    maxx = max(Xs)
    maxy = max(Ys)

    canvas_cols = int(math.ceil(math.ceil(maxx) - math.floor(minx))) + 1
    canvas_lines = int(round(maxy - miny))

    canvas = AsciiCanvas(canvas_cols, canvas_lines)

    # NOTE: first draw edges so that node boxes could overwrite them
    for edge in sug.g.sE:
        # pylint: disable=protected-access
        assert len(edge.view._pts) > 1
        for index in range(1, len(edge.view._pts)):
            start = edge.view._pts[index - 1]
            end = edge.view._pts[index]

            start_x = int(round(start[0] - minx))
            start_y = int(round(start[1] - miny))
            end_x = int(round(end[0] - minx))
            end_y = int(round(end[1] - miny))

            assert start_x >= 0
            assert start_y >= 0
            assert end_x >= 0
            assert end_y >= 0

            canvas.line(start_x, start_y, end_x, end_y, "*")

    for vertex in sug.g.sV:
        # NOTE: moving boxes w/2 to the left
        x = vertex.view.xy[0] - vertex.view.w / 2.0
        y = vertex.view.xy[1]

        canvas.box(
            int(round(x - minx)),
            int(round(y - miny)),
            vertex.view.w,
            vertex.view.h,
        )

        canvas.text(
            int(round(x - minx)) + 1, int(round(y - miny)) + 1, vertex.data
        )

    canvas.draw()
