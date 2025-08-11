import React from "react";

function MLResults({ results }) {
  if (!results || results.length === 0) {
    return <p>No ML results available. Please run the pipeline.</p>;
  }

  return (
    <div className="ml-results">
      <h2>Model Performance Results</h2>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {results.map((res, idx) => (
            <tr key={idx}>
              <td>{res.metric}</td>
              <td>{res.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default MLResults;