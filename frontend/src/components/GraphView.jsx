import React, { useState, useEffect, useRef, useCallback } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import axios from 'axios';
import { X, ZoomIn, ZoomOut, Maximize2 } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const DOMAIN_COLORS = {
  customer_management: '#3B82F6',  // blue
  billing_revenue: '#10B981',      // green
  network_operations: '#F59E0B',   // amber
  general: '#8B5CF6',             // purple
  unknown: '#6B7280',             // gray
};

const GraphView = ({ onClose }) => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [tableDetail, setTableDetail] = useState(null);
  const [loading, setLoading] = useState(true);
  const fgRef = useRef();

  useEffect(() => {
    fetchGraph();
  }, []);

  const fetchGraph = async () => {
    try {
      const res = await axios.get(`${API_BASE}/graph`);
      const { nodes, edges } = res.data;

      // Transform to react-force-graph format
      const graphNodes = nodes.map(n => ({
        id: n.id,
        label: n.label,
        domain: n.domain,
        row_count: n.row_count,
        column_count: n.column_count,
        pii_columns: n.pii_columns,
        color: DOMAIN_COLORS[n.domain] || DOMAIN_COLORS.unknown,
        val: Math.max(3, Math.log10(Math.max(n.row_count, 1)) * 3),
      }));

      const graphLinks = edges.map(e => ({
        source: e.source,
        target: e.target,
        source_column: e.source_column,
        target_column: e.target_column,
        label: `${e.source_column} → ${e.target_column}`,
      }));

      setGraphData({ nodes: graphNodes, links: graphLinks });
    } catch (err) {
      console.error("Failed to fetch graph", err);
    } finally {
      setLoading(false);
    }
  };

  const handleNodeClick = useCallback(async (node) => {
    setSelectedNode(node);
    try {
      const res = await axios.get(`${API_BASE}/graph/table/${node.id}`);
      setTableDetail(res.data);
    } catch (err) {
      console.error("Failed to fetch table detail", err);
    }
  }, []);

  const paintNode = useCallback((node, ctx, globalScale) => {
    const label = node.label;
    const fontSize = Math.max(10 / globalScale, 2);
    const nodeRadius = node.val + 2;

    // Node circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, nodeRadius, 0, 2 * Math.PI);
    ctx.fillStyle = node.color;
    ctx.fill();

    // Glow effect
    ctx.shadowColor = node.color;
    ctx.shadowBlur = 8;
    ctx.strokeStyle = 'rgba(255,255,255,0.3)';
    ctx.lineWidth = 0.5;
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Selected highlight
    if (selectedNode && selectedNode.id === node.id) {
      ctx.beginPath();
      ctx.arc(node.x, node.y, nodeRadius + 3, 0, 2 * Math.PI);
      ctx.strokeStyle = '#FFFFFF';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    // Label
    ctx.font = `${fontSize}px Inter, sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = '#FFFFFF';
    ctx.fillText(label, node.x, node.y + nodeRadius + fontSize + 2);
  }, [selectedNode]);

  const paintLink = useCallback((link, ctx) => {
    ctx.strokeStyle = 'rgba(100, 116, 139, 0.4)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(link.source.x, link.source.y);
    ctx.lineTo(link.target.x, link.target.y);
    ctx.stroke();

    // Arrow
    const dx = link.target.x - link.source.x;
    const dy = link.target.y - link.source.y;
    const angle = Math.atan2(dy, dx);
    const targetR = (link.target.val || 5) + 4;
    const arrowX = link.target.x - Math.cos(angle) * targetR;
    const arrowY = link.target.y - Math.sin(angle) * targetR;

    ctx.fillStyle = 'rgba(100, 116, 139, 0.6)';
    ctx.beginPath();
    ctx.moveTo(arrowX, arrowY);
    ctx.lineTo(
      arrowX - 4 * Math.cos(angle - Math.PI / 6),
      arrowY - 4 * Math.sin(angle - Math.PI / 6)
    );
    ctx.lineTo(
      arrowX - 4 * Math.cos(angle + Math.PI / 6),
      arrowY - 4 * Math.sin(angle + Math.PI / 6)
    );
    ctx.fill();
  }, []);

  return (
    <div className="fixed inset-0 bg-black/80 z-50 flex flex-col">
      {/* Header */}
      <div className="flex justify-between items-center p-4 border-b border-gray-700 bg-gray-900/90">
        <div>
          <h2 className="text-xl font-bold text-white">Knowledge Graph</h2>
          <p className="text-sm text-gray-400">
            {graphData.nodes.length} tables, {graphData.links.length} relationships
          </p>
        </div>

        <div className="flex items-center gap-4">
          {/* Domain Legend */}
          <div className="flex gap-3 text-xs">
            {Object.entries(DOMAIN_COLORS).filter(([k]) => k !== 'unknown').map(([domain, color]) => (
              <span key={domain} className="flex items-center gap-1">
                <span className="w-3 h-3 rounded-full inline-block" style={{ backgroundColor: color }}></span>
                {domain.replace('_', ' ')}
              </span>
            ))}
          </div>

          <div className="flex gap-2">
            <button onClick={() => fgRef.current?.zoomToFit(400, 40)}
                    className="p-2 bg-gray-700 rounded hover:bg-gray-600" title="Fit to view">
              <Maximize2 size={16} />
            </button>
            <button onClick={onClose}
                    className="p-2 bg-gray-700 rounded hover:bg-gray-600 text-red-400">
              <X size={18} />
            </button>
          </div>
        </div>
      </div>

      {/* Graph + Detail Panel */}
      <div className="flex-1 flex">
        {/* Graph Canvas */}
        <div className="flex-1 relative">
          {loading ? (
            <div className="flex items-center justify-center h-full text-gray-400">
              Loading graph...
            </div>
          ) : graphData.nodes.length === 0 ? (
            <div className="flex items-center justify-center h-full text-gray-500">
              No graph data yet. Run the pipeline first to build the knowledge graph.
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              graphData={graphData}
              nodeCanvasObject={paintNode}
              linkCanvasObject={paintLink}
              onNodeClick={handleNodeClick}
              nodePointerAreaPaint={(node, color, ctx) => {
                ctx.beginPath();
                ctx.arc(node.x, node.y, node.val + 5, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();
              }}
              backgroundColor="#0B0F19"
              cooldownTicks={100}
              linkDirectionalArrowLength={0}
              d3VelocityDecay={0.3}
              onEngineStop={() => fgRef.current?.zoomToFit(400, 60)}
            />
          )}
        </div>

        {/* Detail Panel */}
        {selectedNode && tableDetail && (
          <div className="w-96 bg-gray-900 border-l border-gray-700 overflow-y-auto p-5">
            <div className="flex justify-between items-start mb-4">
              <div>
                <h3 className="text-lg font-bold text-white">{selectedNode.label}</h3>
                <span className="text-xs px-2 py-0.5 rounded-full"
                      style={{ backgroundColor: selectedNode.color + '30', color: selectedNode.color }}>
                  {tableDetail.domain}
                </span>
              </div>
              <button onClick={() => { setSelectedNode(null); setTableDetail(null); }}
                      className="text-gray-400 hover:text-white">
                <X size={16} />
              </button>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-2 mb-4">
              <div className="bg-gray-800 p-2 rounded text-center">
                <p className="text-lg font-bold text-blue-400">{selectedNode.row_count?.toLocaleString()}</p>
                <p className="text-xs text-gray-500">Rows</p>
              </div>
              <div className="bg-gray-800 p-2 rounded text-center">
                <p className="text-lg font-bold text-green-400">{selectedNode.column_count}</p>
                <p className="text-xs text-gray-500">Columns</p>
              </div>
              <div className="bg-gray-800 p-2 rounded text-center">
                <p className="text-lg font-bold text-red-400">{selectedNode.pii_columns}</p>
                <p className="text-xs text-gray-500">PII</p>
              </div>
            </div>

            {/* Relationships */}
            {tableDetail.relationships?.length > 0 && (
              <div className="mb-4">
                <h4 className="text-sm font-semibold text-gray-300 mb-2">FK Relationships</h4>
                <div className="space-y-1">
                  {tableDetail.relationships.map((r, i) => (
                    <div key={i} className="text-xs bg-gray-800 p-2 rounded flex justify-between">
                      <span className="text-blue-300">{r.related_table}</span>
                      <span className="text-gray-500">
                        {r.details?.source_column} → {r.details?.target_column}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Downstream */}
            {tableDetail.downstream_tables?.length > 0 && (
              <div className="mb-4">
                <h4 className="text-sm font-semibold text-gray-300 mb-2">Downstream Tables</h4>
                <div className="flex flex-wrap gap-1">
                  {tableDetail.downstream_tables.map(t => (
                    <span key={t} className="text-xs bg-gray-800 px-2 py-1 rounded text-amber-400">{t}</span>
                  ))}
                </div>
              </div>
            )}

            {/* Columns */}
            {tableDetail.schema?.columns?.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-gray-300 mb-2">Columns</h4>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {tableDetail.schema.columns.map((col, i) => (
                    <div key={i} className="text-xs bg-gray-800 p-2 rounded">
                      <div className="flex justify-between">
                        <span className="font-mono text-gray-200">{col.name}</span>
                        <span className="text-gray-500">{col.data_type}</span>
                      </div>
                      {col.pii_classification && col.pii_classification !== 'none' && (
                        <span className="text-red-400 text-[10px]">PII: {col.pii_classification}</span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default GraphView;
