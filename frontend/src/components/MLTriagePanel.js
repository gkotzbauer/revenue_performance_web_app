import React from "react";

function MLTriagePanel({ flaggedCases }) {
  if (!flaggedCases || flaggedCases.length === 0) {
    return <p>No ML triage issues found. Great job!</p>;
  }

  return (
    <div className="ml-triage-panel">
      <h2>ML Triage</h2>
      <p>These records require review after the last pipeline run:</p>
      <ul>
        {flaggedCases.map((caseItem, idx) => (
          <li key={idx}>
            <strong>{caseItem.id}</strong> — {caseItem.issue}
          </li>
        ))}
      </ul>
    </div>
  );
}

export default MLTriagePanel;