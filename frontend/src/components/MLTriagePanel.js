import React from "react";

function MLTriagePanel({ outputs }) {
  // Check if we have pipeline outputs
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
        <p>Upload a file and run the pipeline to see ML triage results.</p>
      </div>
    );
  }

  // Find key output files
  const underpaymentFiles = outputs.filter(file => 
    file.name.includes('underpayment') || 
    file.name.includes('drivers')
  );
  
  const weeklyFiles = outputs.filter(file => 
    file.name.includes('weekly') || 
    file.name.includes('performance')
  );

  const mlFiles = outputs.filter(file => 
    file.name.includes('ml_') || 
    file.name.includes('ML_') || 
    file.name.includes('diagnostics')
  );

  return (
    <div className="ml-triage-panel">
      <div style={{ 
        background: "#fff3cd", 
        padding: "15px", 
        borderRadius: "5px", 
        marginBottom: "20px",
        border: "1px solid #ffeaa7"
      }}>
        <h3 style={{ margin: "0 0 10px 0", color: "#856404" }}>
          🔍 ML Triage Analysis Complete
        </h3>
        <p style={{ margin: 0, color: "#856404" }}>
          Pipeline has analyzed your data for underpayment drivers and ML insights
        </p>
      </div>

      {/* Underpayment Drivers */}
      {underpaymentFiles.length > 0 && (
        <div style={{ 
          background: "white", 
          padding: "15px", 
          borderRadius: "5px",
          marginBottom: "20px",
          border: "1px solid #ddd"
        }}>
          <h4>💰 Underpayment Driver Analysis</h4>
          <p>Identified key areas where revenue gaps exist:</p>
          <ul style={{ listStyle: "none", padding: 0 }}>
            {underpaymentFiles.map((file, idx) => (
              <li key={idx} style={{ 
                padding: "8px 0", 
                borderBottom: idx < underpaymentFiles.length - 1 ? "1px solid #eee" : "none" 
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
      )}

      {/* Weekly Performance */}
      {weeklyFiles.length > 0 && (
        <div style={{ 
          background: "white", 
          padding: "15px", 
          borderRadius: "5px",
          marginBottom: "20px",
          border: "1px solid #ddd"
        }}>
          <h4>📅 Weekly Performance Analysis</h4>
          <p>Performance metrics by week and payer:</p>
          <ul style={{ listStyle: "none", padding: 0 }}>
            {weeklyFiles.map((file, idx) => (
              <li key={idx} style={{ 
                padding: "8px 0", 
                borderBottom: idx < weeklyFiles.length - 1 ? "1px solid #eee" : "none" 
              }}>
                📈 <strong>{file.name}</strong>
                <div style={{ fontSize: "0.9em", color: "#666", marginTop: "4px" }}>
                  Size: {Math.round(file.size_bytes / 1024)} KB • 
                  Modified: {new Date(file.modified_at * 1000).toLocaleString()}
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* ML Diagnostics */}
      {mlFiles.length > 0 && (
        <div style={{ 
          background: "white", 
          padding: "15px", 
          borderRadius: "5px",
          marginBottom: "20px",
          border: "1px solid #ddd"
        }}>
          <h4>🤖 ML Diagnostics & Predictions</h4>
          <p>Machine learning insights and rate predictions:</p>
          <ul style={{ listStyle: "none", padding: 0 }}>
            {mlFiles.map((file, idx) => (
              <li key={idx} style={{ 
                padding: "8px 0", 
                borderBottom: idx < mlFiles.length - 1 ? "1px solid #eee" : "none" 
              }}>
                🧠 <strong>{file.name}</strong>
                <div style={{ fontSize: "0.9em", color: "#666", marginTop: "4px" }}>
                  Size: {Math.round(file.size_bytes / 1024)} KB • 
                  Modified: {new Date(file.modified_at * 1000).toLocaleString()}
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Summary */}
      <div style={{ 
        background: "#d4edda", 
        padding: "15px", 
        borderRadius: "5px",
        border: "1px solid #c3e6cb"
      }}>
        <h4 style={{ margin: "0 0 10px 0", color: "#155724" }}>
          ✅ Triage Summary
        </h4>
        <p style={{ margin: 0, color: "#155724" }}>
          {outputs.length} total output files generated • 
          {underpaymentFiles.length} underpayment analyses • 
          {weeklyFiles.length} performance reports • 
          {mlFiles.length} ML diagnostics
        </p>
      </div>
    </div>
  );
}

export default MLTriagePanel;