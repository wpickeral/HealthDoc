import { useIsAuthenticated, useMsal } from "@azure/msal-react";
import { loginRequest } from "./authConfig";
import { Dashboard } from "./components/Dashboard";

export default function App() {
  const isAuthenticated = useIsAuthenticated();
  const { instance } = useMsal();

  if (isAuthenticated) {
    return <Dashboard />;
  }

  return (
    <div className="login-page">
      <h1>HealthDoc Internal Dashboard</h1>
      <p>Sign in with your organizational account to access the dashboard.</p>
      <button onClick={() => instance.loginPopup(loginRequest)}>Sign In</button>
    </div>
  );
}
