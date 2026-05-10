import { useIsAuthenticated, useMsal } from "@azure/msal-react";
import { InteractionStatus } from "@azure/msal-browser";
import { loginRequest } from "./authConfig";
import { Dashboard } from "./components/Dashboard";

export default function App() {
  const isAuthenticated = useIsAuthenticated();
  const { instance, inProgress } = useMsal();

  if (inProgress !== InteractionStatus.None) {
    return <div className="login-page"><p>Signing in...</p></div>;
  }

  if (isAuthenticated) {
    return <Dashboard />;
  }

  return (
    <div className="login-page">
      <h1>HealthDoc Internal Dashboard</h1>
      <p>Sign in with your organizational account to access the dashboard.</p>
      <button onClick={() => instance.loginRedirect(loginRequest)}>Sign In</button>
    </div>
  );
}
