import { useEffect, useState } from "react";
import { APIM_BASE } from "../authConfig.ts";
import { useApiToken } from "../hooks/useApiToken";

interface FailedFile {
  fileName: string;
  downloadUrl: string;
  uploadedAt: string | null;
}

export function FailedFilesPanel() {
  const { getToken } = useApiToken();
  const [files, setFiles] = useState<FailedFile[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const token = await getToken();
        const res = await fetch(`${APIM_BASE}/failed-files`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setFiles(await res.json());
      } catch (e) {
        setError(String(e));
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) return <p>Loading failed files...</p>;
  if (error) return <p className="error">Error: {error}</p>;
  if (files.length === 0) return <p>No failed files found.</p>;

  return (
    <table>
      <thead>
        <tr>
          <th>File Name</th>
          <th>Uploaded At</th>
          <th>Download</th>
        </tr>
      </thead>
      <tbody>
        {files.map((f) => (
          <tr key={f.fileName}>
            <td>{f.fileName}</td>
            <td>{f.uploadedAt ? new Date(f.uploadedAt).toLocaleString() : "—"}</td>
            <td>
              <a href={f.downloadUrl} target="_blank" rel="noreferrer">
                Download
              </a>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
