import { useState } from "react";
import { APIM_BASE } from "../authConfig";
import { useApiToken } from "../hooks/useApiToken";

interface ProcessedRecord {
  id: string;
  clinicId: string;
  patientId: string;
  testCode: string;
  result: number;
  unit: string;
  referenceRange: string;
  isAbnormal: boolean;
  collectedAt: string;
  processedAt: string;
  status: string;
}

export function ResultsPanel() {
  const { getToken } = useApiToken();
  const [clinicId, setClinicId] = useState("");
  const [records, setRecords] = useState<ProcessedRecord[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function search() {
    if (!clinicId.trim()) return;
    setLoading(true);
    setError(null);
    setRecords(null);
    try {
      const token = await getToken();
      const res = await fetch(`${APIM_BASE}/results/${encodeURIComponent(clinicId)}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setRecords(await res.json());
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <div className="search-bar">
        <input
          type="text"
          placeholder="Clinic ID"
          value={clinicId}
          onChange={(e) => setClinicId(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && search()}
        />
        <button onClick={search} disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {error && <p className="error">Error: {error}</p>}

      {records !== null && records.length === 0 && (
        <p>No records found for clinic "{clinicId}".</p>
      )}

      {records && records.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Patient ID</th>
              <th>Test Code</th>
              <th>Result</th>
              <th>Unit</th>
              <th>Reference Range</th>
              <th>Abnormal</th>
              <th>Collected At</th>
              <th>Processed At</th>
            </tr>
          </thead>
          <tbody>
            {records.map((r) => (
              <tr key={r.id} className={r.isAbnormal ? "abnormal" : ""}>
                <td>{r.patientId}</td>
                <td>{r.testCode}</td>
                <td>{r.result}</td>
                <td>{r.unit}</td>
                <td>{r.referenceRange}</td>
                <td>{r.isAbnormal ? "Yes" : "No"}</td>
                <td>{new Date(r.collectedAt).toLocaleDateString()}</td>
                <td>{new Date(r.processedAt).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
