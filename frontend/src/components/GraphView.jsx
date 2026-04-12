import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import axios from 'axios';
import { Maximize2, X } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const DOMAIN_COLORS = {
  customer_management: '#2563eb',
  billing_revenue: '#16a34a',
  network_operations: '#d97706',
  general: '#7c3aed',
  unknown: '#64748b',
};

const GraphView = ({
  onClose,
  embedded = false,
  sourceName = null,
  runId = null,
  onSelectTable = null,
}) => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [tableDetail, setTableDetail] = useState(null);
  const [loading, setLoading] = useState(true);
  const [containerSize, setContainerSize] = useState({ width: 600, height: 500 });
  const [error, setError] = useState('');
  const fgRef = useRef();
  const containerRef = useRef(null);

  // Track container size so ForceGraph2D receives explicit dimensions,
  // which prevents the "invisible graph" issue on large datasets where
  // the canvas auto-sizes to 0 during the first paint.
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        if (width > 0 && height > 0) {
          setContainerSize({ width, height });
        }
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const fetchGraph = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const params = {};
      if (sourceName) params.source_name = sourceName;
      if (runId) params.run_id = runId;
      const res = await axios.get(`${API_BASE}/graph`, { params });
      const { nodes = [], edges = [], error: apiError } = res.data || {};
      if (apiError) {
        setError(apiError);
      }

      const graphNodes = nodes.map((n) => ({
        id: n.id,
        label: n.label,
        domain: n.domain,
        row_count: n.row_count,
        column_count: n.column_count,
        pii_columns: n.pii_columns,
        color: DOMAIN_COLORS[n.domain] || DOMAIN_COLORS.unknown,
        val: Math.max(4, Math.min(20, Math.log10(Math.max(n.row_count || 0, 1) + 1) * 4 + 4)),
        _focused: Boolean(n._focused),
      }));

      const graphLinks = edges.map((e) => ({
        source: e.source,
        target: e.target,
        source_column: e.source_column,
        target_column: e.target_column,
        label: `${e.source_column} -> ${e.target_column}`,
      }));

      setGraphData({ nodes: graphNodes, links: graphLinks });
    } catch (err) {
      console.error("Failed to fetch graph", err);
      setError('Could not load graph data.');
    } finally {
      setLoading(false);
    }
  }, [sourceName, runId]);

  useEffect(() => {
    fetchGraph();
  }, [fetchGraph]);

  // Fit the graph to view once the simulation settles. Uses a safe timeout
  // fallback for large graphs where onEngineStop may fire before the canvas
  // has a chance to lay out.
  useEffect(() => {
    if (!graphData.nodes.length) return;
    const timer = setTimeout(() => {
      try {
        fgRef.current?.zoomToFit(500, 60);
      } catch {
        // ignore – fitting can briefly fail during rapid remounts
      }
    }, 450);
    return () => clearTimeout(timer);
  }, [graphData]);

  const handleNodeClick = useCallback(async (node) => {
    setSelectedNode(node);
    if (onSelectTable) {
      onSelectTable(node.id);
    }
    try {
      const res = await axios.get(`${API_BASE}/graph/table/${node.id}`);
      setTableDetail(res.data);
    } catch (err) {
      console.error("Failed to fetch table detail", err);
      setTableDetail(null);
    }
  }, [onSelectTable]);

  const paintNode = useCallback((node, ctx, globalScale) => {
    const label = node.label;
    const fontSize = Math.max(10 / globalScale, 2);
    const nodeRadius = (node.val || 5) + 2;

    ctx.beginPath();
    ctx.arc(node.x, node.y, nodeRadius, 0, 2 * Math.PI);
    ctx.fillStyle = node.color;
    ctx.fill();

    ctx.shadowColor = node.color;
    ctx.shadowBlur = node._focused ? 16 : 8;
    ctx.strokeStyle = node._focused ? '#facc15' : 'rgba(255,255,255,0.35)';
    ctx.lineWidth = node._focused ? 2.5 : 0.5;
    ctx.stroke();
    ctx.shadowBlur = 0;

    if (selectedNode && selectedNode.id === node.id) {
      ctx.beginPath();
      ctx.arc(node.x, node.y, nodeRadius + 3, 0, 2 * Math.PI);
      ctx.strokeStyle = '#ffffff';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    ctx.font = `${fontSize}px "Space Grotesk", sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = '#f8fafc';
    ctx.fillText(label, node.x, node.y + nodeRadius + fontSize + 2);
  }, [selectedNode]);

  const paintLink = useCallback((link, ctx) => {
    const sx = link.source?.x;
    const sy = link.source?.y;
    const tx = link.target?.x;
    const ty = link.target?.y;
    if ([sx, sy, tx, ty].some((v) => v == null || Number.isNaN(v))) return;

    ctx.strokeStyle = 'rgba(148, 163, 184, 0.45)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(sx, sy);
    ctx.lineTo(tx, ty);
    ctx.stroke();

    const dx = tx - sx;
    const dy = ty - sy;
    const angle = Math.atan2(dy, dx);
    const targetR = (link.target?.val || 5) + 4;
    const arrowX = tx - Math.cos(angle) * targetR;
    const arrowY = ty - Math.sin(angle) * targetR;

    ctx.fillStyle = 'rgba(148, 163, 184, 0.75)';
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

  const subtitleParts = useMemo(() => {
    const parts = [];
    parts.push(`${graphData.nodes.length} tables`);
    parts.push(`${graphData.links.length} relationships`);
    if (sourceName) parts.push(`source: ${sourceName}`);
    return parts.join(' | ');
  }, [graphData, sourceName]);

  const panel = (
    <div className="flex h-full flex-col overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
      <div className="flex flex-wrap justify-between items-center gap-3 p-5 border-b border-slate-200">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">Knowledge Graph</h2>
          <p className="text-sm text-slate-500">{subtitleParts}</p>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <div className="hidden flex-wrap gap-3 text-xs text-slate-500 xl:flex">
            {Object.entries(DOMAIN_COLORS).filter(([k]) => k !== 'unknown').map(([domain, color]) => (
              <span key={domain} className="flex items-center gap-1">
                <span className="h-2.5 w-2.5 rounded-full inline-block" style={{ backgroundColor: color }}></span>
                {domain.replace(/_/g, ' ')}
              </span>
            ))}
          </div>

          <div className="flex gap-2">
            <button onClick={() => fgRef.current?.zoomToFit(400, 60)}
                    className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100" title="Fit to view">
              <Maximize2 size={16} />
            </button>
            {!embedded && (
              <button onClick={onClose}
                      className="p-2 rounded-lg border border-slate-200 hover:bg-slate-100 text-slate-500">
                <X size={18} />
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="flex-1 flex min-h-[400px]">
        <div
          ref={containerRef}
          className="flex-1 relative bg-[#0b1020] min-h-[400px]"
        >
          {loading ? (
            <div className="flex items-center justify-center h-full text-slate-300">
              Loading graph...
            </div>
          ) : graphData.nodes.length === 0 ? (
            <div className="flex items-center justify-center h-full text-slate-400 p-6 text-center">
              {error || 'No graph data for this source yet. Run the pipeline first.'}
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              width={containerSize.width}
              height={containerSize.height}
              graphData={graphData}
              nodeCanvasObject={paintNode}
              linkCanvasObject={paintLink}
              onNodeClick={handleNodeClick}
              nodePointerAreaPaint={(node, color, ctx) => {
                ctx.beginPath();
                ctx.arc(node.x, node.y, (node.val || 5) + 5, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();
              }}
              backgroundColor="#0b1020"
              cooldownTicks={Math.min(200, Math.max(80, graphData.nodes.length * 4))}
              linkDirectionalArrowLength={0}
              d3VelocityDecay={0.35}
              warmupTicks={20}
              onEngineStop={() => {
                try {
                  fgRef.current?.zoomToFit(400, 60);
                } catch {
                  // ignore
                }
              }}
            />
          )}
        </div>

        {selectedNode && tableDetail && (
          <div className="w-96 border-l border-slate-200 overflow-y-auto p-5">
            <div className="flex justify-between items-start mb-4">
              <div>
                <h3 className="text-lg font-semibold text-slate-900">{selectedNode.label}</h3>
                <span className="text-xs px-2 py-0.5 rounded-full"
                      style={{ backgroundColor: selectedNode.color + '20', color: selectedNode.color }}>
                  {tableDetail.domain}
                </span>
              </div>
              <button onClick={() => { setSelectedNode(null); setTableDetail(null); }}
                      className="text-slate-400 hover:text-slate-900">
                <X size={16} />
              </button>
            </div>

            <div className="grid grid-cols-3 gap-2 mb-4">
              <div className="bg-slate-50 p-2 rounded text-center">
                <p className="text-lg font-semibold text-blue-600">{selectedNode.row_count?.toLocaleString()}</p>
                <p className="text-xs text-slate-500">Rows</p>
              </div>
              <div className="bg-slate-50 p-2 rounded text-center">
                <p className="text-lg font-semibold text-emerald-600">{selectedNode.column_count}</p>
                <p className="text-xs text-slate-500">Columns</p>
              </div>
              <div className="bg-slate-50 p-2 rounded text-center">
                <p className="text-lg font-semibold text-rose-600">{selectedNode.pii_columns}</p>
                <p className="text-xs text-slate-500">PII</p>
              </div>
            </div>

            {tableDetail.relationships?.length > 0 && (
              <div className="mb-4">
                <h4 className="text-sm font-semibold text-slate-700 mb-2">FK Relationships</h4>
                <div className="space-y-1">
                  {tableDetail.relationships.map((r, i) => (
                    <div key={i} className="text-xs bg-slate-50 p-2 rounded flex justify-between">
                      <span className="text-blue-600">{r.related_table}</span>
                      <span className="text-slate-500">
                        {r.details?.source_column} {" -> "} {r.details?.target_column}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {tableDetail.downstream_tables?.length > 0 && (
              <div className="mb-4">
                <h4 className="text-sm font-semibold text-slate-700 mb-2">Downstream Tables</h4>
                <div className="flex flex-wrap gap-1">
                  {tableDetail.downstream_tables.map(t => (
                    <span key={t} className="text-xs bg-slate-50 px-2 py-1 rounded text-amber-600">{t}</span>
                  ))}
                </div>
              </div>
            )}

            {tableDetail.schema?.columns?.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-slate-700 mb-2">Columns</h4>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {tableDetail.schema.columns.map((col, i) => (
                    <div key={i} className="text-xs bg-slate-50 p-2 rounded">
                      <div className="flex justify-between">
                        <span className="font-mono text-slate-700">{col.name}</span>
                        <span className="text-slate-500">{col.data_type}</span>
                      </div>
                      {col.pii_classification && col.pii_classification !== 'none' && (
                        <span className="text-rose-600 text-[10px]">PII: {col.pii_classification}</span>
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

  if (embedded) {
    return panel;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-6">
      <div className="h-[90vh] w-full max-w-6xl">{panel}</div>
    </div>
  );
};

export default GraphView;
