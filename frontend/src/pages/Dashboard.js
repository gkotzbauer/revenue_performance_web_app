import React, { useState, useEffect } from "react";
import MLResults from "../components/MLResults";
import MLTriagePanel from "../components/MLTriagePanel";
import HelpGuide from "../components/HelpGuide";

const Dashboard = () => {
  const [outputs, setOutputs] = useState([]);
  const [logs, setLogs] = useState([]);

  // Fetch list of generated output files from backend
  useEffect(() => {
    fetch("/api/list-outputs")
      .then((res) => res.json())
      .then((data) => setOutputs(data.files || []))
      .catch((err) => console.error("Error fetching outputs:", err));

    fetch("/api/logs")
      .then((res) => res.json())
      .then((data) => setLogs(data.logs || []))
      .catch((err) => console.error("Error fetching logs:", err));
  }, []);

  const downloadFile = (filename) => {
    window.open(`/api/download/${encodeURIComponent(filename)}`, "_blank");
  };

  return (
    <div style={{ padding: "20px" }}>
      <h1>📊 Revenue Performance Dashboard</h1>

      {/* Output Files */}
      <section style={{ marginBottom: "30px" }}>
        <h2>Available Output Files</h2>
        {outputs.length > 0 ? (
          <ul>
            {outputs.map((file, idx) => (
              <li key={idx}>
                {file}{" "}
                <button
                  style={{
                    background: "#007bff",
                    color: "#fff",
                    border: "none",
                    padding: "4px 8px",
                    cursor: "pointer",
                  }}
                  onClick={() => downloadFile(file)}
                >
                  Download
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p>No output files generated yet.</p>
        )}
      </section>

      {/* Logs */}
      <section style={{ marginBottom: "30px" }}>
        <h2>Pipeline Logs</h2>
        <div
          style={{
            background: "#f8f9fa",
            padding: "10px",
            border: "1px solid #ccc",
            maxHeight: "300px",
            overflowY: "auto",
          }}
        >
          {logs.length > 0 ? (
            logs.map((log, idx) => <pre key={idx}>{log}</pre>)
          ) : (
            <p>No logs yet.</p>
          )}
        </div>
      </section>

      {/* ML Results */}
      <section style={{ marginBottom: "30px" }}>
        <h2>Machine Learning Results</h2>
        <MLResults />
      </section>

      {/* ML Triage Panel */}
      <section style={{ marginBottom: "30px" }}>
        <h2>ML Triage Panel</h2>
        <MLTriagePanel />
      </section>

      {/* Help Guide */}
      <section>
        <h2>Help / ML Guide</h2>
        <HelpGuide />
      </section>
    </div>
  );
};

export default Dashboard;