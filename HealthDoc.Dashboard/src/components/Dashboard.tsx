import { useState } from "react";
import { useAccount } from "@azure/msal-react";
import { FailedFilesPanel } from "./FailedFilesPanel";
import { ResultsPanel } from "./ResultsPanel";

type Tab = "failed" | "results";

export function Dashboard() {
  const account = useAccount();
  const [activeTab, setActiveTab] = useState<Tab>("failed");

  return (
    <div className="dashboard">
      <header>
        <h1>HealthDoc Internal Dashboard</h1>
        <span className="user-info">{account?.username}</span>
      </header>

      <nav className="tabs">
        <button
          className={activeTab === "failed" ? "active" : ""}
          onClick={() => setActiveTab("failed")}
        >
          Failed Files
        </button>
        <button
          className={activeTab === "results" ? "active" : ""}
          onClick={() => setActiveTab("results")}
        >
          Lab Results
        </button>
      </nav>

      <main>
        {activeTab === "failed" ? <FailedFilesPanel /> : <ResultsPanel />}
      </main>
    </div>
  );
}
