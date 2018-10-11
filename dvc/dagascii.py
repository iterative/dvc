import sys
import math
import select

from grandalf.graphs import Vertex, Edge, Graph
from grandalf.layouts import SugiyamaLayout
from grandalf.routing import route_with_lines, EdgeViewer


class AsciiCanvas(object):
    def __init__(self, cols, lines):
        assert cols > 1
        assert lines > 1

        self.cols = cols
        self.lines = lines

        self.canvas = [[' '] * cols for l in range(lines)]

    def draw(self):
        if sys.stdout.isatty():  # pragma: no cover
            from asciimatics.screen import Screen
            Screen.wrapper(self._do_draw)
        else:
            for line in self.canvas:
                print(''.join(line))

    def _do_draw(self, screen):  # pragma: no cover
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
                    if len(self.canvas) > y_index \
                       and len(self.canvas[y_index]) > x_index:
                            line.append(self.canvas[y_index][x_index])
                    else:
                        line.append(' ')
                assert len(line) == (smaxcol + 1)
                screen.print_at(''.join(line), 0, y)

            screen.refresh()

            # NOTE: get_event() doesn't block by itself,
            # so we have to do the blocking ourselves.
            select.select([sys.stdin], [], [], None)

            event = screen.get_event()
            if not isinstance(event, KeyboardEvent):
                continue

            k = event.key_code
            if k == screen.KEY_DOWN or k == ord('s'):
                offset_y += 1
            elif k == screen.KEY_PAGE_DOWN or k == ord('S'):
                offset_y += smaxrow
            elif k == screen.KEY_UP or k == ord('w'):
                offset_y -= 1
            elif k == screen.KEY_PAGE_UP or k == ord('W'):
                offset_y -= smaxrow
            elif k == screen.KEY_RIGHT or k == ord('d'):
                offset_x += 1
            elif k == ord('D'):
                offset_x += smaxcol
            elif k == screen.KEY_LEFT or k == ord('a'):
                offset_x -= 1
            elif k == ord('A'):
                offset_x -= smaxcol
            elif k == ord('q') or k == ord('Q'):
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
        assert x >= 0
        assert x < self.cols
        assert y >= 0
        assert y < self.lines

        self.canvas[y][x] = char

    def line(self, x0, y0, x1, y1, char):
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
        for i, c in enumerate(text):
            self.point(x + i, y, c)

    def box(self, x0, y0, w, h):
        assert w > 1
        assert h > 1

        w -= 1
        h -= 1

        for x in range(x0, x0 + w):
            self.point(x, y0, '-')
            self.point(x, y0 + h, '-')

        for y in range(y0, y0 + h):
            self.point(x0, y, '|')
            self.point(x0 + w, y, '|')

        self.point(x0, y0, '+')
        self.point(x0 + w, y0, '+')
        self.point(x0, y0 + h, '+')
        self.point(x0 + w, y0 + h, '+')


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

                canvas.line(start_x, start_y,
                            end_x, end_y,
                            '*')

        for v in self.sug.g.sV:
            # NOTE: moving boxes w/2 to the left
            x = v.view.xy[0] - v.view.w/2.0
            y = v.view.xy[1]

            canvas.box(int(round(x - minx)),
                       int(round(y - miny)),
                       v.view.w,
                       v.view.h)

            canvas.text(int(round(x - minx)) + 1,
                        int(round(y - miny)) + 1,
                        v.data)

        canvas.draw()
