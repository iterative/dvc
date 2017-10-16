# -*- coding: utf-8 -*-

TAIL = r'''
  </script>

  <div class="container centered">
    <div id="myGraph">
    </div>
  </div>

  <!-- d3 scripts -->
  <script>
    // exposed variables
    var attrs = {
      svgWidth: 1000,
      svgHeight: 600,
      marginTop: 10,
      marginBottom: 150,
      marginRight: 50,
      marginLeft: 150,
      container: 'body',
      data: null,
      nodesCnfg: {
        horizontalDistanceBetweenNodes: 500,
        verticalDistanceBetweenNodes: 200,
        nodeWidth: 480,
        nodeTextHeight: 20,
        nodeTextWidth: 250
      },
      metronikColors: {
        nodeFill: "#E9EDEF",
        nodeRed: "#E35B5A",
        nodeGreen: "#26C281",
        nodeBlack: "#2F353B",
        nodeText: "#22313F",
        nodeLink: "#2F353B"
      }

    };

    Array.prototype.orderBy = function (func) {
      this.sort((a, b) => {

        var a = func(a);
        var b = func(b);

        if (typeof a === 'string' || a instanceof String) {
          return a.localeCompare(b);
        }
        return a - b;
      });
      return this;
    }



    // setting parents and children
    data.links.forEach(d => {
      var nodes = data.nodes;
      var first = nodes.filter(n => d.source == n.id)[0]
      var second = nodes.filter(n => d.target == n.id)[0]
      var parent = first.sequence > second.sequence ? first : second;
      var child = first.sequence < second.sequence ? first : second;
      if (!child.parents) {
        child.parents = [];
      }
      child.parents.push(parent);
      if (!parent.children) {
        parent.children = [];
      }
      parent.children.push(child);
    })

    function setChildrenLevels(arr, level) {
      arr.forEach(d => {

        if (d.verticalLevel == undefined || d.verticalLevel < level) d.verticalLevel = level;


        if (d.children) {
          setChildrenLevels(d.children, level + 1);
        }
      })
    }

    function setParentLevels(arr, level) {
      arr.forEach(d => {

        if (d.verticalLevel != undefined || d.verticalLevel < level) d.verticalLevel = level;


        if (d.parents) {
          setParentLevels(d.parents, level - 1);
        }
      })
    }

    function setHorizontalSequence(node) {

      if (!node.parents) return;
      var parents = node.parents;
      // var sorted = parents.orderBy(d => d.sequence);
      var sorted = parents.orderBy(d => d.children ? d.children.length : 0);
      sorted.forEach((d, i) => {
        d.horSeq = i;
        d.horSeqChild = node.horSeq;
        setHorizontalSequence(d);

      })
    }


    //set initial vertical levels
    setChildrenLevels(data.nodes.filter(d => !d.parents), 1);

    //get max vertical level and it's node
    var maxValueVerticalLevel = d3.max(data.nodes, d => d.verticalLevel);
    var minNodes = data.nodes.filter(d => d.verticalLevel == maxValueVerticalLevel);
    node = minNodes;

    //set parent levels
    setParentLevels(minNodes, maxValueVerticalLevel);
    var levelNodes = {}
    //create level nodes array
    data.nodes.forEach(d => {
      if (!levelNodes[d.verticalLevel]) {
        levelNodes[d.verticalLevel] = [];
      }
      levelNodes[d.verticalLevel].push(d);
    })

    // set horizontal level sequences
    minNodes.forEach((d, i) => { d.horSeq = i; })
    minNodes.forEach(d => setHorizontalSequence(d))

    // reorder levels by child levels and apply new level
    Object.keys(levelNodes).forEach(k => {
      var arr = levelNodes[k];
      //  arr.orderBy(d => d.horSeqChild);
      arr.forEach((d, i) => d.horizontalLevel = i)
    })

    attrs.data = convertJsonFormat(calculateDataCnfg(data));
    renderChart();


    function renderChart() {

      // setNodeVerticalLevels(attrs.data);

      //calculated properties
      var calc = {}
      calc.chartLeftMargin = attrs.marginLeft;
      calc.chartTopMargin = attrs.marginTop;
      calc.chartWidth = attrs.svgWidth - attrs.marginRight - calc.chartLeftMargin;
      calc.chartHeight = attrs.svgHeight - attrs.marginBottom - calc.chartTopMargin;


      //drawing containers
      var container = d3.select("#myGraph");

      var width = attrs.nodesCnfg.horizontalDistanceBetweenNodes * attrs.data.maxNodesNumberByLevel +
        (attrs.nodesCnfg.horizontalDistanceBetweenNodes / 2) * (attrs.data.maxNodesNumberByLevel - 1)

      //add svg
      var svg = patternify({ container: container, selector: 'svg-chart-container', elementTag: 'svg' })
        .attr('width', width)
        .attr('height', attrs.svgHeight)
        .attr('overflow', 'visible')
        .style('font-family', 'Helvetica')
        .style('font-size', '12pt');

      //add container g element
      var chart = patternify({ container: svg, selector: 'chart', elementTag: 'g' })
      chart.attr('transform', 'translate(' + (calc.chartLeftMargin + width / 2) + ',' + calc.chartTopMargin + ')');


      // link lines group
      var linksGroup = patternify({ container: chart, selector: 'links-group', elementTag: 'g' })
      var links = patternify({ container: linksGroup, selector: 'link-line', elementTag: 'line', data: d => attrs.data.links })

      links
        .attr("x1", d => calculateNodeXCord(attrs.data.nodes.filter(i => i.id == d.source)[0]))
        .attr("x2", d => calculateNodeXCord(attrs.data.nodes.filter(i => i.id == d.target)[0]))
        .attr("y1", d => attrs.data.nodes.filter(i => i.id == d.source)[0].verticalLevel * attrs.nodesCnfg.verticalDistanceBetweenNodes)
        .attr("y2", d => attrs.data.nodes.filter(i => i.id == d.target)[0].verticalLevel * attrs.nodesCnfg.verticalDistanceBetweenNodes)
        .attr("stroke-width", 2)
        .attr("stroke", attrs.metronikColors.nodeLink)


      // node group for link line, ellpise and texts
      var nodeGroups = patternify({ container: chart, selector: 'node-group', elementTag: 'g', data: d => { return attrs.data.nodes } })
      nodeGroups.attr('transform', function (d) {
        var x = calculateNodeXCord(d);
        return 'translate(' + (x) + ',' + (d.verticalLevel * attrs.nodesCnfg.verticalDistanceBetweenNodes) + ')'
      });


      //node ellipces
      // var ellipses = patternify({ container: nodeGroups, selector: 'ellipse', elementTag: 'ellipse', data: d => [d] })
      // ellipses.attr("cx", 0)
      //     .attr("cy", 0)
      //     .attr("rx", attrs.nodesCnfg.nodeWidth / 2)
      //     .attr("ry", d => (d.strings.length+2) * attrs.nodesCnfg.nodeTextHeight)
      //     .attr("stroke", function(d){
      //            if(d.color == "red"){  return attrs.metronikColors.nodeRed; }
      //            else if(d.color == "green") { return attrs.metronikColors.nodeGreen; }
      //            else{  return attrs.metronikColors.nodeBlack }
      //     })
      //     .attr("stroke-width", 3)
      //     .attr("fill", attrs.metronikColors.nodeFill)

      var rects = patternify({ container: nodeGroups, selector: 'rect', elementTag: 'rect', data: d => [d] })
      rects.attr("rx", 10)
        .attr("ry", 10)
        .attr("x", -attrs.nodesCnfg.nodeWidth / 2)
        .attr("y", d => -((d.strings.length) * attrs.nodesCnfg.nodeTextHeight + 30) / 2)
        .attr("width", attrs.nodesCnfg.nodeWidth)
        .attr("height", d => ((d.strings.length) * attrs.nodesCnfg.nodeTextHeight) + 30)
        .attr("stroke", function (d) {
          return getNodeColor(d)
        })
        .attr("stroke-width", 3)
        .attr("fill", attrs.metronikColors.nodeFill)



      // texts group for several text in one node
      var textsGroup = patternify({ container: nodeGroups, selector: 'node-text-group', elementTag: 'g', data: d => [d] })
      textsGroup.attr('transform', d => 'translate(' + (-attrs.nodesCnfg.nodeWidth * 0.4) + ',' + (- d.strings.length * attrs.nodesCnfg.nodeTextHeight * 0.4) + ')');

      var targetNumber = patternify({ container: textsGroup, selector: 'target-name-number', elementTag: 'text', data: d => [d] })
      targetNumber.text(d => d.targetNumber == null ? "" : "(" + d.targetNumber + ")")
        .attr("fill", function (d) {
          return getNodeColor(d)
        })
        .attr("transform", (d, i) => "translate(" + (getTextWidth(d.strings[1], "12pt Helvetica")) + "," + attrs.nodesCnfg.nodeTextHeight + ")")

      var texts = textsGroup.selectAll(".node-text")
        .data(d => d.strings)
        .enter()
        .append('text')
        .attr("class", "node-text")
        .text(d => d)
        .style("font-weight", (d, i) => i == 0 ? "bold" : "")
        .style("color", attrs.metronikColors.nodeText)
        .attr("transform", (d, i) => "translate(" + (i == 0 ? 40 : 0) + "," + (i * attrs.nodesCnfg.nodeTextHeight) + ")")

        .each(wrap)

      svg.attr('height', attrs.nodesCnfg.verticalDistanceBetweenNodes * attrs.data.maxLevel)

    }



    function getHeadNodeIds() {
      Array.prototype.diff = function (a) {
        return this.filter(function (i) {
          return a.indexOf(i) === -1;
        });
      };

      var nodeIds = attrs.data.nodes.map(function (d) { return d.id; })
      var targets = attrs.data.links.map(function (d) { return d.target })

      var uniqueTargets = [...new Set(targets)];
      var headNodeIds = nodeIds.diff(uniqueTargets);

      return headNodeIds;
    }

    function convertJsonFormat(data) {
      for (var i = 0; i < data.nodes.length; i++) {
        if (data.nodes[i].hasOwnProperty("branches")) {
          data.nodes[i]["strings"] = []
          data.nodes[i]["strings"][0] = "BRANCH TIPS: " + data.nodes[i].branches.join(", ")
        }
        for (var j = 0; j < data.nodes[i].commits.length; j++) {
          data.nodes[i]["strings"] = data.nodes[i]["strings"] || [""];
          data.nodes[i]["strings"].push("[" + data.nodes[i].commits[j].hash + "] " + data.nodes[i].commits[j].text);
        }
        if (data.nodes[i].collapsed_commits_number != undefined)
          data.nodes[i]["strings"].push("<<" + data.nodes[i].collapsed_commits_number + " collapsed commits>>")
      }
      return data;
    }

    function getLongestPathFromHeadNode() {
      var headNodeIds = getHeadNodeIds();

      var headNodePath = headNodeIds.map(function (id) {

      })


    }

    function getNodeColor(d) {
      if (d.targetNumber != undefined) {
        return +(d.targetNumber) > 0 ? attrs.metronikColors.nodeGreen : attrs.metronikColors.nodeRed
      }
      return attrs.metronikColors.nodeBlack;
    }

    function calculateNodeXCord(d) {
      var xTranslate = 0;

      if (d.nodesMaxNumberInLevel != 1) {
        if (d.nodesMaxNumberInLevel % 2 == 0) {

          if (d.horizontalLevel < Math.floor(d.nodesMaxNumberInLevel / 2)) {
            xTranslate = attrs.nodesCnfg.horizontalDistanceBetweenNodes / 2 +
              (Math.floor(d.nodesMaxNumberInLevel / 2) - d.horizontalLevel - 1) * (attrs.nodesCnfg.horizontalDistanceBetweenNodes)

            xTranslate = 0 - xTranslate;
          } else {
            xTranslate = attrs.nodesCnfg.horizontalDistanceBetweenNodes / 2 +
              (d.horizontalLevel - Math.floor(d.nodesMaxNumberInLevel / 2)) * (attrs.nodesCnfg.horizontalDistanceBetweenNodes)
          }
        }
        else {

          if (Math.floor(d.nodesMaxNumberInLevel / 2) != d.horizontalLevel) {

            if (d.horizontalLevel < Math.floor(d.nodesMaxNumberInLevel / 2)) {
              xTranslate = attrs.nodesCnfg.nodeWidth / 2 + attrs.nodesCnfg.horizontalDistanceBetweenNodes / 2 +
                (Math.floor(d.nodesMaxNumberInLevel / 2) - d.horizontalLevel - 1) * (attrs.nodesCnfg.horizontalDistanceBetweenNodes)

              xTranslate = 0 - xTranslate;
            } else {

              xTranslate = attrs.nodesCnfg.nodeWidth / 2 + attrs.nodesCnfg.horizontalDistanceBetweenNodes / 2 +
                (d.horizontalLevel - Math.floor(d.nodesMaxNumberInLevel / 2) - 1) * (attrs.nodesCnfg.horizontalDistanceBetweenNodes)

            }

          }
        }
      }
      return xTranslate;
    }

    function wrap() {
      var self = d3.select(this),
        textLength = self.node().getComputedTextLength(),
        text = self.text();
      while (getTextWidth(text, "12pt Helvetica") > attrs.nodesCnfg.nodeTextWidth + 40) {
        text = text.slice(0, -1);
        self.text(text + '...');
        //textLength = self.node().getComputedTextLength();
      }
    }

    function getTextWidth(text, font) {
      // if given, use cached canvas for better performance
      // else, create new canvas
      var canvas = getTextWidth.canvas || (getTextWidth.canvas = document.createElement("canvas"));
      var context = canvas.getContext("2d");
      context.font = font;
      var metrics = context.measureText(text);
      return metrics.width;
    };

    //enter exit update pattern principle
    function patternify(params) {
      var container = params.container;
      var selector = params.selector;
      var elementTag = params.elementTag;
      var data = params.data || [selector];

      // pattern in action
      var selection = container.selectAll('.' + selector).data(data)
      selection.exit().remove();
      selection = selection.enter().append(elementTag).merge(selection)
      selection.attr('class', selector);
      return selection;

    }

    function calculateDataCnfg(callbackdata) {
      var groupedBylevel = d3.nest()
        .key(function (d) { return d.verticalLevel; })
        .rollup(function (leaves) { return leaves.length; })
        .entries(callbackdata.nodes);

      var data = callbackdata.nodes.map(function (d) {
        d.nodesMaxNumberInLevel = groupedBylevel.filter(i => i.key == d.verticalLevel)[0].value
        return d;
      })

      callbackdata.nodes = data;

      var maxNodesNumberByLevel = Math.max.apply(null, groupedBylevel.map(function (d) { return d.value }));
      callbackdata.maxNodesNumberByLevel = maxNodesNumberByLevel;
      callbackdata.maxLevel = Math.max.apply(null, callbackdata.nodes.map(function (d) { return d.verticalLevel })) + 1;
      return callbackdata;
    }
  </script>

</body>

</html>
'''
