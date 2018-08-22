import math

from asciicanvas.asciicanvas import AsciiCanvas
from asciicanvas.style import Style

from grandalf.graphs import Vertex, Edge, Graph
from grandalf.layouts import SugiyamaLayout
from grandalf.routing import route_with_lines, EdgeViewer


class Dagascii(object):
    def __init__(self, vertexes, edges):
        #
        # Just a reminder about naming conventions:
        # +------------X
        # |
        # |
        # |
        # |
        # Y
        #

        V = {v: Vertex(" {} ".format(v)) for v in vertexes}
        # NOTE: reverting edges to correctly orientate the graph
        E = [Edge(V[e], V[s]) for s, e in edges]
        V = V.values()
        g = Graph(V, E)

        class VertexViewer(object):
            h = 3  # top and bottom box edges + text

            def __init__(self, name):
                self.w = len(name) + 2  # right and left bottom edges + text

        for v in V:
            v.view = VertexViewer(v.data)

        # NOTE: determine min box length to create the best layout
        minw = min([v.view.w for v in V])

        for e in E:
            e.view = EdgeViewer()

        sug = SugiyamaLayout(g.C[0])
        gr = g.C[0]
        r = list(filter(lambda x: len(x.e_in()) == 0, gr.sV))

        sug.init_all(roots=r, optimize=True)

        sug.yspace = VertexViewer.h
        sug.xspace = minw
        sug.route_edge = route_with_lines

        sug.draw()

        self.sug = sug

    def draw(self):
        # NOTE: coordinates might me negative, so we need to shift
        # everything to the positive plane before we actually draw it.
        Xs = []
        Ys = []

        for v in self.sug.g.sV:
            # NOTE: moving boxes w/2 to the left
            Xs.append(v.view.xy[0] - v.view.w/2.0)
            Xs.append(v.view.xy[0] + v.view.w/2.0)
            Ys.append(v.view.xy[1])
            Ys.append(v.view.xy[1] + v.view.h)

        for e in self.sug.g.sE:
            for xy in e.view._pts:
                Xs.append(xy[0])
                Ys.append(xy[1])

        minx = min(Xs)
        miny = min(Ys)
        maxx = max(Xs)
        maxy = max(Ys)

        canvas_cols = int(math.ceil(math.ceil(maxx) - math.floor(minx))) + 1
        canvas_lines = int(round(maxy - miny))

        canvas = AsciiCanvas(canvas_cols, canvas_lines)

        # NOTE: first draw edges so that node boxes could overwrite them
        for e in self.sug.g.sE:
            assert len(e.view._pts) > 1
            for index in range(1, len(e.view._pts)):
                start = e.view._pts[index - 1]
                end = e.view._pts[index]

                start_x = int(round(start[0] - minx))
                start_y = int(round(start[1] - miny))
                end_x = int(round(end[0] - minx))
                end_y = int(round(end[1] - miny))

                assert start_x >= 0
                assert start_y >= 0
                assert end_x >= 0
                assert end_y >= 0

                canvas.add_line(start_x, start_y,
                                end_x, end_y,
                                style=Style('*'))

        for v in self.sug.g.sV:
            # NOTE: moving boxes w/2 to the left
            x = v.view.xy[0] - v.view.w/2.0
            y = v.view.xy[1]

            canvas.add_nine_patch_rect(int(round(x - minx)),
                                       int(round(y - miny)),
                                       v.view.w,
                                       v.view.h)

            canvas.add_text(int(round(x - minx)) + 1,
                            int(round(y - miny)) + 1,
                            v.data)

        canvas.print_out()
