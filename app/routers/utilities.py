'use client';

import { useState } from "react";
import styles from "@/styles/AddUtilityModal.module.css";

interface Props {
  hotelId: string;
  onClose: () => void;
  onSave?: () => void;
}

export default function AddUtilityModal({ hotelId, onClose, onSave }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState("");
  const [detectedType, setDetectedType] = useState<string | null>(null);
  const [manualType, setManualType] = useState<string>("");

  const pollJobStatus = async (jobId: string, label: string) => {
    for (let attempt = 0; attempt < 20; attempt++) {
      const res = await fetch(`https://app.docupanda.io/job/${jobId}`, {
        headers: {
          "accept": "application/json",
          "X-API-Key": process.env.NEXT_PUBLIC_DOCUPANDA_API_KEY || "", // set in .env
        },
      });

      if (!res.ok) throw new Error(`${label} job failed to fetch`);

      const data = await res.json();
      if (data.status === "completed") return true;
      if (data.status === "error") throw new Error(`${label} job failed`);
      await new Promise((r) => setTimeout(r, 3000));
    }
    throw new Error(`${label} job timed out`);
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0] || null;
    setFile(selected);
    setStatus("");
    setDetectedType(null);
    setManualType("");

    if (!selected) return;

    const formData = new FormData();
    formData.append("file", selected);

    setStatus("⏳ Checking file type...");

    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/utilities/precheck`, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) throw new Error("Precheck failed");

      const data = await res.json();
      if (data.bill_type === "electricity" || data.bill_type === "gas") {
        setDetectedType(data.bill_type);
        setStatus(`✅ Detected: ${data.bill_type} bill`);
      } else {
        setDetectedType("unknown");
        setStatus("⚠️ Unknown bill type — please select manually.");
      }
    } catch (err: any) {
      console.error(err);
      setStatus("❌ Failed to check bill type");
      setDetectedType("unknown");
    }
  };

  const handleSubmit = async () => {
    if (!file) {
      alert("Please select a file.");
      return;
    }

    const utilityType = detectedType !== "unknown" ? detectedType : manualType;
    if (!utilityType) {
      alert("Please select a utility type.");
      return;
    }

    setUploading(true);
    setStatus("⏳ Uploading file to DocuPanda...");

    const formData = new FormData();
    formData.append("file", file);
    formData.append("hotel_id", hotelId);
    formData.append("supplier", "docupanda");
    formData.append("utility_type", utilityType);

    try {
      const uploadRes = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/utilities/parse-and-save`, {
        method: "POST",
        body: formData,
      });

      if (!uploadRes.ok) throw new Error("Upload failed");
      const {
        upload_job_id,
        standardization_job_id,
        standardization_id,
        document_id,
        filename,
        bill_type,
      } = await uploadRes.json();

      setStatus("⏳ Waiting for DocuPanda to process the file...");
      await pollJobStatus(upload_job_id, "Upload");
      await pollJobStatus(standardization_job_id, "Standardization");

      setStatus("✅ Finalizing and saving parsed data...");
      const finalizeData = new FormData();
      finalizeData.append("document_id", document_id);
      finalizeData.append("standardization_id", standardization_id);
      finalizeData.append("hotel_id", hotelId);
      finalizeData.append("bill_type", bill_type);
      finalizeData.append("filename", filename);

      const finalizeRes = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/utilities/finalize`, {
        method: "POST",
        body: finalizeData,
      });

      if (!finalizeRes.ok) {
        const errData = await finalizeRes.json();
        throw new Error(errData.detail || "Finalize failed");
      }

      setStatus("✅ Upload complete. Dashboard will refresh shortly.");
      onSave?.();
      setTimeout(onClose, 2000);
    } catch (err: any) {
      console.error(err);
      setStatus(`❌ Error: ${err.message}`);
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className={styles.overlay}>
      <div className={styles.modal}>
        <div className={styles.header}>
          <h2>Upload Utility Bill</h2>
          <button onClick={onClose}>✕</button>
        </div>

        <div className={styles.body}>
          <input type="file" accept="application/pdf" onChange={handleFileChange} />
          {file && <p>📄 {file.name}</p>}
          {status && <p>{status}</p>}

          {detectedType === "unknown" && (
            <div>
              <label>Select Utility Type:</label>
              <select
                value={manualType}
                onChange={(e) => setManualType(e.target.value)}
                disabled={uploading}
              >
                <option value="">-- Select --</option>
                <option value="electricity">Electricity</option>
                <option value="gas">Gas</option>
              </select>
            </div>
          )}
        </div>

        <div className={styles.footer}>
          <button
            className={styles.uploadButton}
            onClick={handleSubmit}
            disabled={!file || uploading}
          >
            {uploading ? "Uploading..." : "Upload Bill"}
          </button>
        </div>
      </div>
    </div>
  );
}
