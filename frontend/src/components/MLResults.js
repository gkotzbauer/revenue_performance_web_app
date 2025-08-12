import React from "react";

function MLResults({ outputs, pipelineSummary }) {
  // Check if we have ML-related outputs - look for actual pipeline output files
  const mlOutputs = outputs.filter(file => 
    file.name.includes('ml_rate_diagnostics') ||
    file.name.includes('ML_Expected_Rate') ||
    file.name.includes('ML_Rate_Gap') ||
    file.name.includes('ML_Materiality_Flag')
  );

  // Also include files that contain ML insights
  const mlInsightFiles = outputs.filter(file => 
    file.name.includes('pipeline_summary') ||
    file.name.includes('performance_narratives') ||
    file.name.includes('processed_invoice_data')
  );

  // Combine all ML-related files
  const allMLFiles = [...mlOutputs, ...mlInsightFiles];

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

  if (allMLFiles.length === 0) {
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
          {allMLFiles.length} ML-related output file{allMLFiles.length !== 1 ? 's' : ''} generated
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
          {pipelineSummary.ml_model_performance && Object.keys(pipelineSummary.ml_model_performance).length > 0 && (
            <div style={{ marginTop: "15px", padding: "10px", background: "white", borderRadius: "4px" }}>
              <h5>🤖 ML Model Performance</h5>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: "8px" }}>
                <div><strong>Model Type:</strong> {pipelineSummary.ml_model_performance.model_type || 'N/A'}</div>
                <div><strong>R² Score:</strong> {pipelineSummary.ml_model_performance.r2_score?.toFixed(3) || 'N/A'}</div>
                <div><strong>MAE:</strong> {pipelineSummary.ml_model_performance.mean_absolute_error?.toFixed(3) || 'N/A'}</div>
                <div><strong>Training Samples:</strong> {pipelineSummary.ml_model_performance.training_samples || 'N/A'}</div>
              </div>
            </div>
          )}
        </div>
      )}

      <div style={{ 
        background: "white", 
        padding: "15px", 
        borderRadius: "5px",
        border: "1px solid #ddd"
      }}>
        <h4>ML Diagnostics & Analysis Generated</h4>
        <ul style={{ listStyle: "none", padding: 0 }}>
          {allMLFiles.map((file, idx) => (
            <li key={idx} style={{ 
              padding: "8px 0", 
              borderBottom: idx < allMLFiles.length - 1 ? "1px solid #eee" : "none" 
            }}>
              {file.name.includes('ml_rate_diagnostics') ? '🧠' : 
               file.name.includes('pipeline_summary') ? '📊' : 
               file.name.includes('performance_narratives') ? '📝' : '📊'} 
              <strong>{file.name}</strong>
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