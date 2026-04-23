import React, { useState, useCallback, useMemo } from 'react';
import Editor from '@monaco-editor/react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  Node,
  Edge,
  MarkerType
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Play, Share2, History, Database } from 'lucide-react';

const initialCode = `# Welcome to Heddle Playground
# Define a schema for your data
schema User:
    id: int
    name: string
    email: string

# Define a workflow
workflow main:
    # Fetch data from a source
    data = io:read_csv("users.csv", schema=User)
    
    # Transform data
    data | filter(id > 100)
         | transform(name = name.upper())
    
    # Write output
    data | io:write_json("processed_users.json")
`;

const initialNodes: Node[] = [
  {
    id: '1',
    type: 'input',
    data: { label: 'io:read_csv' },
    position: { x: 250, y: 5 },
  },
  {
    id: '2',
    data: { label: 'filter' },
    position: { x: 250, y: 100 },
  },
  {
    id: '3',
    data: { label: 'transform' },
    position: { x: 250, y: 200 },
  },
  {
    id: '4',
    type: 'output',
    data: { label: 'io:write_json' },
    position: { x: 250, y: 300 },
  },
];

const initialEdges: Edge[] = [
  { id: 'e1-2', source: '1', target: '2', animated: true, markerEnd: { type: MarkerType.ArrowClosed } },
  { id: 'e2-3', source: '2', target: '3', animated: true, markerEnd: { type: MarkerType.ArrowClosed } },
  { id: 'e3-4', source: '3', target: '4', animated: true, markerEnd: { type: MarkerType.ArrowClosed } },
];

export default function App() {
  const [code, setCode] = useState(initialCode);
  const [nodes, setNodes] = useState(initialNodes);
  const [edges, setEdges] = useState(initialEdges);

  const onEditorChange = (value: string | undefined) => {
    if (value) setCode(value);
    // In a real version, we'd trigger a parser/compiler update here to update the DAG
  };

  return (
    <div className="flex flex-col h-screen w-screen overflow-hidden bg-[#0D1117] text-[#C9D1D9]">
      {/* Navbar */}
      <header className="flex items-center justify-between px-6 py-3 border-b border-[#30363D] bg-[#161B22]">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-[#58A6FF] rounded flex items-center justify-center">
            <span className="text-[#0D1117] font-bold">H</span>
          </div>
          <h1 className="text-xl font-semibold tracking-tight">Heddle <span className="text-[#8B949E] font-normal">Playground</span></h1>
        </div>

        <div className="flex items-center gap-4">
          <button className="flex items-center gap-2 px-4 py-2 bg-[#238636] hover:bg-[#2EA043] text-white rounded-md text-sm font-medium transition-colors">
            <Play size={16} /> Run
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-[#21262D] hover:bg-[#30363D] border border-[#30363D] rounded-md text-sm font-medium transition-colors">
            <Share2 size={16} /> Share
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex flex-1 overflow-hidden">
        {/* Editor Pane */}
        <div className="w-1/2 flex flex-col border-r border-[#30363D]">
          <div className="flex items-center gap-4 px-4 py-2 bg-[#161B22] border-b border-[#30363D]">
            <span className="text-xs font-medium uppercase tracking-wider text-[#8B949E]">main.he</span>
          </div>
          <div className="flex-1 overflow-hidden">
            <Editor
              height="100%"
              defaultLanguage="python" // Temporary until we have Heddle syntax highlighting
              theme="vs-dark"
              value={code}
              onChange={onEditorChange}
              options={{
                fontSize: 14,
                fontFamily: 'JetBrains Mono',
                minimap: { enabled: false },
                scrollBeyondLastLine: false,
                padding: { top: 16 },
                lineNumbersMinChars: 3,
              }}
            />
          </div>
        </div>

        {/* Visualizer Pane */}
        <div className="w-1/2 flex flex-col bg-[#0D1117]">
          <div className="flex items-center justify-between px-4 py-2 bg-[#161B22] border-b border-[#30363D]">
            <div className="flex gap-4">
              <button className="text-xs font-medium uppercase tracking-wider text-[#58A6FF] border-b-2 border-[#58A6FF] pb-1">
                DAG View
              </button>
              <button className="text-xs font-medium uppercase tracking-wider text-[#8B949E] hover:text-[#C9D1D9] transition-colors pb-1">
                Data Preview
              </button>
            </div>
            <div className="flex gap-2">
              <button title="History" className="p-1.5 hover:bg-[#30363D] rounded transition-colors text-[#8B949E]">
                <History size={16} />
              </button>
              <button title="Resources" className="p-1.5 hover:bg-[#30363D] rounded transition-colors text-[#8B949E]">
                <Database size={16} />
              </button>
            </div>
          </div>

          <div className="flex-1">
            <ReactFlow
              nodes={nodes}
              edges={edges}
              fitView
            >
              <Background color="#30363D" gap={20} />
              <Controls />
              <MiniMap />
            </ReactFlow>
          </div>
        </div>
      </main>

      {/* Footer / Console */}
      <footer className="h-32 border-t border-[#30363D] bg-[#0D1117] px-6 py-3">
        <div className="flex items-center gap-2 mb-2">
          <span className="text-xs font-medium uppercase tracking-wider text-[#8B949E]">Console</span>
        </div>
        <div className="font-mono text-sm text-[#8B949E]">
          <p>[02:25:34] <span className="text-[#58A6FF]">INFO</span> Initializing Heddle Control Plane...</p>
          <p>[02:25:35] <span className="text-[#58A6FF]">INFO</span> Connected to worker-1 (Go Runtime)</p>
          <p>[02:25:35] <span className="text-[#2EA043]">SUCCESS</span> Ready for execution.</p>
        </div>
      </footer>
    </div>
  );
}
