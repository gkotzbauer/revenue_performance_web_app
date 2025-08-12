import React, { useState, useEffect } from "react";
import MLResults from "../components/MLResults";
import MLTriagePanel from "../components/MLTriagePanel";

const Dashboard = () => {
  const [outputs, setOutputs] = useState([]);
  const [pipelineSummary, setPipelineSummary] = useState(null);

  // Fetch list of generated output files and pipeline summary from backend
  useEffect(() => {
    fetch("/api/download/list")
      .then((res) => res.json())
      .then((data) => setOutputs(data.files || []))
      .catch((err) => console.error("Error fetching outputs:", err));

    // Try to fetch pipeline summary if it exists
    fetch("/api/download/file?filename=pipeline_summary.json")
      .then((res) => res.json())
      .then((data) => setPipelineSummary(data))
      .catch((err) => console.log("No pipeline summary available yet"));
  }, []);

  const downloadFile = (filename) => {
    window.open(`/api/download/file?filename=${encodeURIComponent(filename)}`, "_blank");
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (timestamp) => {
    return new Date(timestamp * 1000).toLocaleString();
  };

  return (
    <div style={{ padding: "20px" }}>
      <h1>📊 Revenue Performance Dashboard</h1>

      {/* ML Results */}
      <section style={{ marginBottom: "30px" }}>
        <h2>🤖 Machine Learning Results</h2>
        <MLResults outputs={outputs} pipelineSummary={pipelineSummary} />
      </section>

      {/* ML Triage Panel */}
      <section style={{ marginBottom: "30px" }}>
        <h2>🔍 ML Triage Panel</h2>
        <MLTriagePanel outputs={outputs} />
        
        {/* Output Files moved here */}
        <div style={{ marginTop: "30px" }}>
          <h3>📁 Available Output Files</h3>
          {outputs.length > 0 ? (
            <div style={{ background: "#f8f9fa", padding: "15px", borderRadius: "5px" }}>
              <p style={{ marginBottom: "15px", color: "#666" }}>
                {outputs.length} file{outputs.length !== 1 ? 's' : ''} available for download
              </p>
              <div style={{ display: "grid", gap: "10px" }}>
                {outputs.map((file, idx) => (
                  <div key={idx} style={{ 
                    display: "flex", 
                    justifyContent: "space-between", 
                    alignItems: "center",
                    padding: "10px",
                    background: "white",
                    border: "1px solid #ddd",
                    borderRadius: "4px"
                  }}>
                    <div>
                      <strong>{file.name}</strong>
                      <div style={{ fontSize: "0.9em", color: "#666" }}>
                        Size: {formatFileSize(file.size_bytes)} • 
                        Modified: {formatDate(file.modified_at)}
                      </div>
                    </div>
                    <button
                      style={{
                        background: "#28a745",
                        color: "#fff",
                        border: "none",
                        padding: "8px 16px",
                        cursor: "pointer",
                        borderRadius: "4px",
                        fontWeight: "bold"
                      }}
                      onClick={() => downloadFile(file.relpath)}
                    >
                      📥 Download
                    </button>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div style={{ 
              background: "#f8f9fa", 
              padding: "20px", 
              borderRadius: "5px",
              textAlign: "center",
              color: "#666"
            }}>
              <p>No output files generated yet.</p>
              <p>Upload a file and run the pipeline to generate outputs.</p>
            </div>
          )}
        </div>
      </section>
    </div>
  );
};

export default Dashboard;