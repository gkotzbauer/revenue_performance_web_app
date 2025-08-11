import React from "react";

const HelpGuide = () => {
  return (
    <div className="help-guide" style={{ maxWidth: 900, margin: "0 auto" }}>
      <h2>Help / ML Guide</h2>
      <p>
        This app runs a multi-step pipeline on your uploaded invoice data to
        measure performance vs two benchmarks: <strong>85% E/M</strong> and a
        historical <strong>peer benchmark</strong>. It also fits an ML model to
        estimate the expected payment <em>rate per visit</em> and highlights
        under/over-payment drivers.
      </p>

      <h3>How to Use</h3>
      <ol>
        <li>Go to <strong>Upload</strong> and upload your CSV/XLSX file.</li>
        <li>Click <strong>Run Pipeline</strong>. This executes all backend steps.</li>
        <li>Open <strong>Dashboard</strong> to review results and download outputs.</li>
        <li>Check <strong>ML Triage</strong> for items flagged by the model.</li>
      </ol>

      <h3>Key Outputs</h3>
      <ul>
        <li>
          <strong>Granular weekly (Benchmark Key)</strong> — performance at
          CPT-set granularity.
        </li>
        <li>
          <strong>Aggregated weekly (Payer / E/M)</strong> — macro view with
          expected vs benchmark payments.
        </li>
        <li>
          <strong>Underpayment Drivers</strong> — where gaps come from by payer,
          key, and week.
        </li>
        <li>
          <strong>ML Diagnostics</strong> — model-expected rate per visit, gaps,
          materiality flags.
        </li>
        <li>
          <strong>Narratives</strong> — “What went well / can be improved”
          generated from metrics and ML.
        </li>
      </ul>

      <h3>Common Questions</h3>
      <details>
        <summary>What’s “Expected_Payment”?</summary>
        <p>
          Expected_Payment = <code>Expected_Amount_85_EM_invoice_level × Visit_Count</code>.
          It’s the 85% E/M expected rate per visit multiplied by the number of
          unique invoices in the period.
        </p>
      </details>
      <details>
        <summary>What’s “benchmark_payment”?</summary>
        <p>
          A weighted historical payment benchmark: the historical{" "}
          <em>payment rate per visit</em> is learned for each Benchmark Key and
          multiplied by the current period’s <code>Visit_Count</code>.
        </p>
      </details>
      <details>
        <summary>How does the ML model help?</summary>
        <p>
          The ML model estimates an <em>expected rate per visit</em> from your
          features. The difference between actual and expected (
          <code>ML_Rate_Gap</code>) is turned into dollars (
          <code>ML_Dollar_Gap</code>) using visit volume, and flagged if above a
          materiality threshold.
        </p>
      </details>

      <h3>Troubleshooting</h3>
      <ul>
        <li>
          Ensure column names in your data match what the pipeline expects (see
          the README).
        </li>
        <li>
          If a step fails, check the <strong>Logs</strong> panel and validate
          that the previous step’s output file exists.
        </li>
        <li>
          For model concerns (high MAE / low R²), review the ML Triage and
          residual analysis CSVs and consider adding more predictive features.
        </li>
      </ul>

      <h3>Full Documentation</h3>
      <p>
        See the in-repo docs:
        <br />
        <a href="/docs/README.html" target="_blank" rel="noreferrer">
          Product README
        </a>{" "}
        |{" "}
        <a href="/docs/ML_GUIDE.html" target="_blank" rel="noreferrer">
          ML Guide
        </a>{" "}
        |{" "}
        <a href="/docs/DEPLOYMENT.html" target="_blank" rel="noreferrer">
          Deployment Guide
        </a>
      </p>
    </div>
  );
};

export default HelpGuide;