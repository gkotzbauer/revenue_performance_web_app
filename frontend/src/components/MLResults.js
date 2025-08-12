import React from "react";

function MLResults({ outputs, pipelineSummary }) {
  // Check if we have ML-related outputs
  const mlOutputs = outputs.filter(file => 
    file.name.includes('ml_') || 
    file.name.includes('ML_') || 
    file.name.includes('diagnostics')
  );

  if (!outputs || outputs.length === 0) {
    return (
      <div style={{ 
        background: "#f8f9fa", 
        padding: "20px", 
        borderRadius: "5px",
        textAlign: "center",
        color: "#666"
      }}>
        <p>No pipeline outputs available yet.</p>
        <p>Upload a file and run the pipeline to see ML results.</p>
      </div>
    );
  }

  if (mlOutputs.length === 0) {
    return (
      <div style={{ 
        background: "#f8f9fa", 
        padding: "20px", 
        borderRadius: "5px",
        textAlign: "center",
        color: "#666"
      }}>
        <p>Pipeline completed but no ML diagnostics found.</p>
        <p>Check the output files below for detailed results.</p>
      </div>
    );
  }

  return (
    <div className="ml-results">
      <div style={{ 
        background: "#e7f3ff", 
        padding: "15px", 
        borderRadius: "5px", 
        marginBottom: "20px",
        border: "1px solid #b3d9ff"
      }}>
        <h3 style={{ margin: "0 0 10px 0", color: "#0056b3" }}>
          🎉 ML Pipeline Results Available
        </h3>
        <p style={{ margin: 0, color: "#0056b3" }}>
          {mlOutputs.length} ML-related output file{mlOutputs.length !== 1 ? 's' : ''} generated
        </p>
      </div>

      {pipelineSummary && (
        <div style={{ 
          background: "#f8f9fa", 
          padding: "15px", 
          borderRadius: "5px",
          marginBottom: "20px"
        }}>
          <h4>Pipeline Summary</h4>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "10px" }}>
            <div><strong>Total Transactions:</strong> {pipelineSummary.total_transactions?.toLocaleString() || 'N/A'}</div>
            <div><strong>Total Charges:</strong> ${pipelineSummary.total_charges?.toLocaleString() || 'N/A'}</div>
            <div><strong>Total Payments:</strong> ${pipelineSummary.total_payments?.toLocaleString() || 'N/A'}</div>
            <div><strong>Collection Rate:</strong> {pipelineSummary.overall_collection_rate?.toFixed(1) || 'N/A'}%</div>
          </div>
        </div>
      )}

      <div style={{ 
        background: "white", 
        padding: "15px", 
        borderRadius: "5px",
        border: "1px solid #ddd"
      }}>
        <h4>ML Diagnostics Generated</h4>
        <ul style={{ listStyle: "none", padding: 0 }}>
          {mlOutputs.map((file, idx) => (
            <li key={idx} style={{ 
              padding: "8px 0", 
              borderBottom: idx < mlOutputs.length - 1 ? "1px solid #eee" : "none" 
            }}>
              📊 <strong>{file.name}</strong>
              <div style={{ fontSize: "0.9em", color: "#666", marginTop: "4px" }}>
                Size: {Math.round(file.size_bytes / 1024)} KB • 
                Modified: {new Date(file.modified_at * 1000).toLocaleString()}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default MLResults;