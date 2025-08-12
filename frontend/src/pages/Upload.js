import React, { useState } from "react";

const Upload = () => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState("");
  const [processing, setProcessing] = useState(false);

  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
    setUploadStatus("");
  };

  const handleUpload = async () => {
    if (!selectedFile) {
      setUploadStatus("❌ Please select a file first.");
      return;
    }

    const formData = new FormData();
    formData.append("file", selectedFile);

    try {
      setProcessing(true);
      setUploadStatus("📤 Uploading file...");

      const res = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) throw new Error(`HTTP error! Status: ${res.status}`);

      const data = await res.json();
      setUploadStatus(`✅ File uploaded successfully: ${data.filename}`);

      // Optionally start pipeline
      setUploadStatus("⚙️ Starting pipeline processing...");
      const pipelineRes = await fetch("/api/pipeline/run", { method: "POST" });
      if (!pipelineRes.ok)
        throw new Error(`Pipeline failed! Status: ${pipelineRes.status}`);

      const pipelineData = await pipelineRes.json();
      
      if (pipelineData.status === "ok") {
        setUploadStatus(`✅ Pipeline completed successfully! Return code: ${pipelineData.return_code}`);
        
        // Check if there are outputs available
        try {
          const outputsRes = await fetch("/api/download/list");
          if (outputsRes.ok) {
            const outputs = await outputsRes.json();
            if (outputs.files && outputs.files.length > 0) {
              setUploadStatus(`✅ Pipeline completed! Generated ${outputs.files.length} output files. Check the Downloads page to view and download your results.`);
            } else {
              setUploadStatus(`✅ Pipeline completed! No output files found yet. Check the Downloads page for updates.`);
            }
          }
        } catch (err) {
          console.log("Could not check outputs:", err);
        }
      } else {
        setUploadStatus(`❌ Pipeline failed with return code: ${pipelineData.return_code}`);
      }
    } catch (err) {
      console.error("Upload error:", err);
      setUploadStatus(`❌ Error: ${err.message}`);
    } finally {
      setProcessing(false);
    }
  };

  return (
    <div style={{ padding: "20px" }}>
      <h1>📤 Upload Data File</h1>
      <p>
        Please upload your invoice-level or weekly model input file (CSV or
        Excel). After upload, the pipeline will run automatically.
      </p>

      <input type="file" onChange={handleFileChange} accept=".csv,.xlsx" />

      <div style={{ marginTop: "10px" }}>
        <button
          onClick={handleUpload}
          disabled={processing}
          style={{
            background: "#28a745",
            color: "#fff",
            padding: "8px 12px",
            border: "none",
            cursor: "pointer",
          }}
        >
          {processing ? "Processing..." : "Upload & Run Pipeline"}
        </button>
      </div>

      {uploadStatus && (
        <div style={{ marginTop: "15px", fontWeight: "bold" }}>
          {uploadStatus}
        </div>
      )}
    </div>
  );
};

export default Upload;
